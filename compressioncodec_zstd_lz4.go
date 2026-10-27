package orc

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// maxLz4ChunkSize caps the buffer grown to hold a decompressed LZ4 block.
// ORC stores raw LZ4 blocks without the uncompressed length, so the size is
// discovered by retrying; the cap bounds memory on a corrupt header.
const maxLz4ChunkSize = 32 << 20

// sharedZstdReader is safe for concurrent DecodeAll and avoids rebuilding a
// decoder per chunk.
var sharedZstdReader = func() *zstd.Decoder {
	d, _ := zstd.NewReader(nil)
	return d
}()

// CompressionZstd decodes ORC streams compressed with ZSTD.
type CompressionZstd struct{}

// Encoder implements the CompressionCodec interface. Writing is not supported.
func (c CompressionZstd) Encoder(w io.Writer) io.WriteCloser {
	return unsupportedEncoder{kind: "ZSTD"}
}

// Decoder implements the CompressionCodec interface.
func (c CompressionZstd) Decoder(r io.Reader) io.Reader {
	return &chunkDecoder{source: r, decompress: zstdDecompressChunk}
}

func zstdDecompressChunk(src []byte) ([]byte, error) {
	return sharedZstdReader.DecodeAll(src, nil)
}

// CompressionLz4 decodes ORC streams compressed with the LZ4 block format.
type CompressionLz4 struct{}

// Encoder implements the CompressionCodec interface. Writing is not supported.
func (c CompressionLz4) Encoder(w io.Writer) io.WriteCloser {
	return unsupportedEncoder{kind: "LZ4"}
}

// Decoder implements the CompressionCodec interface.
func (c CompressionLz4) Decoder(r io.Reader) io.Reader {
	return &chunkDecoder{source: r, decompress: lz4DecompressChunk}
}

func lz4DecompressChunk(src []byte) ([]byte, error) {
	size := len(src) * 4
	if size < 1024 {
		size = 1024
	}
	for {
		dst := make([]byte, size)
		n, err := lz4.UncompressBlock(src, dst)
		if err == nil {
			return dst[:n], nil
		}
		if size >= maxLz4ChunkSize {
			return nil, err
		}
		size *= 2
	}
}

// chunkDecoder reads ORC's generic compression stream: a sequence of chunks,
// each prefixed by a 3-byte little-endian header whose low bit marks an
// uncompressed (original) chunk and whose upper bits hold the chunk length.
// Compressed chunks are read whole and handed to decompress; this mirrors the
// snappy decoder, which also cannot use its format's native framing.
type chunkDecoder struct {
	source     io.Reader
	decompress func([]byte) ([]byte, error)
	decoded    io.Reader
}

func (c *chunkDecoder) readHeader() (int, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(c.source, header[:3]); err != nil {
		return 0, err
	}
	headerVal := binary.LittleEndian.Uint32(header)
	isOriginal := headerVal%2 == 1
	chunkLength := int(headerVal / 2)
	if isOriginal {
		c.decoded = io.LimitReader(c.source, int64(chunkLength))
		return 0, nil
	}
	src, err := io.ReadAll(io.LimitReader(c.source, int64(chunkLength)))
	if err != nil {
		return 0, err
	}
	decoded, err := c.decompress(src)
	if err != nil {
		return 0, err
	}
	c.decoded = bytes.NewReader(decoded)
	return 0, nil
}

func (c *chunkDecoder) Read(p []byte) (int, error) {
	if c.decoded == nil {
		return c.readHeader()
	}
	n, err := c.decoded.Read(p)
	if err == io.EOF {
		c.decoded = nil
		return n, nil
	}
	return n, err
}

type unsupportedEncoder struct{ kind string }

func (u unsupportedEncoder) Write(p []byte) (int, error) {
	return 0, fmt.Errorf("orc: %s encoding not implemented", u.kind)
}

func (u unsupportedEncoder) Close() error {
	return fmt.Errorf("orc: %s encoding not implemented", u.kind)
}
