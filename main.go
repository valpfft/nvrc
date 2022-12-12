package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	ffmpeg "github.com/u2takey/ffmpeg-go"
	bolt "go.etcd.io/bbolt"
)

// 1. register multiple folders, each camera for example.
// 2. Create a queue of files, sort by date of creation.
// 3. Using ffmpeg convert an oldest file, save it to tmp directory.
// 4. Once it complete, verify it somehow and move into a folder in place of original file.
// 5. Remove it from queue.
//
// How to check if file is already processed? Maintain a list/ write a queue into the db?
// Log errors somewhere, maybe send a tg notification if job is failed. Or report once for a while.
//
// ffmpeg command: ffmpeg -i 2022-12-11T18-15-00.mp4 -vcodec libx264 -crf 27 -b:v 1024k  -bufsize 1024k -s 1920x1080 /tmp/2022-12-11T18-15-00.mp4
//
// Once older files are proceeded, create a more future proof solution:
// Monitor folders using fs.Notify
// once new file arrived - transcode it.
// On a new server storage, introduce a layered scheme:
// 1. Record files from shinobi to SSD
// 2. Transcode them and save into archive on nas storage.
//
type dirs []string

func (d *dirs) Set(value string) error {
	*d = append(*d, value)
	return nil
}

func (d *dirs) String() string {
	return fmt.Sprintf("[%s]", strings.Join(*d, ","))
}

type Output struct {
	Resolution string
	CRF        int
	Preset     string
}

type Compressor struct {
	db     *bolt.DB
	log    *zerolog.Logger
	output Output
}

func main() {
	var drs dirs
	var resolution, preset string
	var crf int
	flag.Var(&drs, "dir", "Specify dirs to watch.")
	flag.StringVar(&resolution, "resolution", "1920x1080", "Specify output video resolution.")
	flag.StringVar(&preset, "preset", "veryfast", "Specify preset.")
	flag.IntVar(&crf, "crf", 27, "Specify crf.")
	flag.Parse()

	db, err := bolt.Open("index.db", 0666, nil)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	logger := log.With().Caller().Logger()

	c := Compressor{
		db:  db,
		log: &logger,
		output: Output{
			Resolution: resolution,
			CRF:        crf,
			Preset:     preset,
		},
	}

	ctx := context.Background()

	for _, d := range drs {
		dir, err := os.Open(d)
		if err != nil {
			panic(err)
		}

		if err := c.IndexDir(ctx, dir); err != nil {
			panic(err)
		}
		if err := c.Work(ctx, dir); err != nil {
			panic(err)
		}
	}
}

type Status int

const (
	TODO = iota
	DONE
)

type File struct {
	Path        string
	Name        string
	CreatedAt   time.Time
	Status      Status
	InitialSize int64
	Size        int64
}

func (c *Compressor) IndexDir(ctx context.Context, d *os.File) error {
	tx, err := c.db.Begin(true)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	c.log.Info().Msgf("creating a bucket: %s", d.Name())
	b, err := tx.CreateBucketIfNotExists([]byte(d.Name()))
	if err != nil {
		return err
	}

	files, err := d.Readdir(0)

	for _, f := range files {
		// Skip this file if it's already indexed!
		k := f.Name()
		if exist := b.Get([]byte(k)); exist != nil {
			continue
		}

		file := &File{
			Path:        d.Name() + "/" + f.Name(),
			Name:        f.Name(),
			CreatedAt:   f.ModTime(),
			Status:      TODO,
			InitialSize: f.Size(),
			Size:        f.Size(),
		}

		buf, err := json.Marshal(file)
		if err != nil {
			return err
		}

		if err := b.Put([]byte(k), buf); err != nil {
			return err
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

// TODO: Work on dirs concurrently.
// 1. Transcode file and save it to tmp directory.
// 2. Move it in place of original file.
// 3. Update metadata in our index, set status to DONE.
// 4. Remove temp file.
func (c *Compressor) Work(ctx context.Context, dir *os.File) error {
	return c.db.View(func(tx *bolt.Tx) error {
		bucket := []byte(dir.Name())
		b := tx.Bucket(bucket)

		return b.ForEach(func(k, v []byte) error {
			c.log.Info().Msgf("processing a file: %s", string(k))

			f := File{}
			if err := json.Unmarshal(v, &f); err != nil {
				return err
			}
			if f.Status == DONE {
				c.log.Info().Msgf("Skipping already processed file: %s", f.Name)
				return nil
			}
			c.log.Info().Msgf("Initial size is %d", f.InitialSize)

			err := ffmpeg.Input(f.Path).
				Output(f.Name, ffmpeg.KwArgs{
					"c:v":    "libx264",
					"preset": c.output.Preset,
					"crf":    c.output.CRF,
					"s":      c.output.Resolution,
				}).
				OverWriteOutput().ErrorToStdOut().Run()

			if err != nil {
				return err
			}

			f.Status = DONE
			fi, err := os.Stat(f.Name)
			if err != nil {
				return err
			}
			f.Size = fi.Size()
			c.log.Info().Int64("initial size", f.InitialSize).Int64("current size", f.Size).Msg("DONE")
			buf, err := json.Marshal(f)
			if err != nil {
				return err
			}
			if err := c.Put([]byte(dir.Name()), k, buf); err != nil {
				return err
			}

			origin, err := os.OpenFile(f.Path, os.O_WRONLY|os.O_TRUNC, 0666)
			if err != nil {
				return err
			}
			defer origin.Close()

			replacement, err := os.Open(f.Name)
			if err != nil {
				return err
			}
			defer replacement.Close()

			_, err = io.Copy(origin, replacement)
			if err != nil {
				return fmt.Errorf("cannot copy content of replacement into origin: %v", err)
			}

			// If copy is OK, then delete temporary file.
			if err := os.Remove(f.Name); err != nil {
				return fmt.Errorf("cannot remove temporary file: %v", err)
			}

			return nil

		})
	})
}

func (c *Compressor) Put(bucket, key, value []byte) error {
	return c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		return b.Put(key, value)
	})
}

func (c *Compressor) GetFile(bucket, key, value []byte) (*File, error) {
	var buf []byte
	if err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucket)
		buf = b.Get(key)
		return nil
	}); err != nil {
		return nil, err
	}

	f := File{}
	if err := json.Unmarshal(buf, &f); err != nil {
		return nil, err
	}

	return &f, nil
}
