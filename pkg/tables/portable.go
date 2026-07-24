package tables

import (
	"bytes"
	"fmt"
	"io"

	"github.com/evrblk/fenestra"
	"github.com/evrblk/monstera/store"
)

// Portable snapshots (see monstera docs/snapshot-and-restore.md): a snapshot
// produced by any core of an application is restorable by any other core of
// the same application, which keeps only the entities in its own shard bounds.
// Only PRIMARY entities are streamed; secondary indexes are rebuilt on restore
// by inserting each entity through the owning table's normal write path.
//
// The split of responsibilities:
//   - Tables (PortableTable) own key layout, entity codec, and index rebuild.
//   - This helper owns the fenestra stream format, section dispatch, and
//     transaction chunking.
//   - The core owns the section list (the wire names are a compatibility
//     contract) and its shard range.

// ShardRange is a shard's inclusive key range. During restore every table
// computes each entity's shard key itself (it knows its keyspace's sharding
// function) and keeps the entity iff Owns accepts it — a fully generic
// ownership check, no per-core predicate required.
type ShardRange struct {
	Lower []byte
	Upper []byte
}

// Owns reports whether shardKey falls within the range (both ends inclusive).
func (r ShardRange) Owns(shardKey []byte) bool {
	return bytes.Compare(shardKey, r.Lower) >= 0 && bytes.Compare(shardKey, r.Upper) <= 0
}

// PortableTable is one section of a core's portable snapshot.
type PortableTable interface {
	// EachEntity streams every primary entity of the table as (canonical key,
	// stored value): the key carries no table id and no producing-shard
	// identity, the value is the stored bytes verbatim.
	EachEntity(txn *store.Txn, fn func(key []byte, value []byte) (bool, error)) error

	// RestoreEntity decodes one streamed entity and, if its recomputed shard
	// key falls within bounds, inserts it through the table's normal write
	// path — re-deriving every key and rebuilding every secondary index under
	// this table's own ids. Reports whether the entity was kept.
	RestoreEntity(txn *store.Txn, key []byte, value []byte, bounds ShardRange) (bool, error)

	// Clear deletes every row this table owns — primary and secondary index
	// rows alike (restore's replace semantics; also how a retired shard's
	// data is reclaimed).
	Clear(badgerStore *store.BadgerStore) error
}

// Section pairs a snapshot table name (part of the stream compatibility
// contract — never derived, always declared by the core) with the table that
// serves it.
type Section struct {
	Name  string
	Table PortableTable
}

// restoreTxnChunk bounds how many entities are inserted per Badger
// transaction during restore (Badger transactions have a size limit).
const restoreTxnChunk = 512

// Snapshot is a consistent, portable snapshot of one core's primary entities
// (it implements monstera.ApplicationCoreSnapshot). It holds no reference to
// the core — only the pinned view and the section list — so it is shared by
// every core.
type Snapshot struct {
	txn      *store.Txn
	coreName string
	sections []Section
}

// NewSnapshot pins a consistent view of the store and captures the section
// list. Write streams from the pinned view concurrently with subsequent
// updates; Release discards it.
func NewSnapshot(badgerStore *store.BadgerStore, coreName string, sections []Section) *Snapshot {
	return &Snapshot{
		txn:      badgerStore.View(),
		coreName: coreName,
		sections: sections,
	}
}

func (s *Snapshot) Write(w io.Writer) error {
	return WriteSnapshot(w, s.txn, s.coreName, s.sections)
}

func (s *Snapshot) Release() {
	s.txn.Discard()
}

// WriteSnapshot streams every section into w as one portable fenestra
// snapshot. txn pins the consistent view; the caller owns its lifecycle.
func WriteSnapshot(w io.Writer, txn *store.Txn, coreName string, sections []Section) error {
	fw := fenestra.NewWriter(w)
	if err := fw.Meta("core", []byte(coreName)); err != nil {
		return err
	}

	for _, sec := range sections {
		if err := fw.Table(sec.Name, true); err != nil {
			return err
		}
		err := sec.Table.EachEntity(txn, func(key []byte, value []byte) (bool, error) {
			if err := fw.Record(key, value); err != nil {
				return false, err
			}
			return true, nil
		})
		if err != nil {
			return err
		}
	}

	return fw.Close()
}

// RestoreSnapshot replaces a core's state with the union of the entities
// from the given streams that fall within bounds: it clears every section's
// table once, then dispatches each streamed record to its section's table,
// committing in chunks. Callers pass one stream (Raft restore, split
// seeding) or two (merge seeding); streams are disjoint after bounds
// filtering, so their order is irrelevant.
func RestoreSnapshot(badgerStore *store.BadgerStore, sections []Section, bounds ShardRange, readers ...io.ReadCloser) error {
	defer func() {
		for _, reader := range readers {
			_ = reader.Close()
		}
	}()

	for _, sec := range sections {
		if err := sec.Table.Clear(badgerStore); err != nil {
			return err
		}
	}

	byName := make(map[string]PortableTable, len(sections))
	for _, sec := range sections {
		byName[sec.Name] = sec.Table
	}

	txn := badgerStore.Update()
	defer func() { txn.Discard() }()
	pending := 0
	commit := func() error {
		if err := txn.Commit(); err != nil {
			return err
		}
		txn = badgerStore.Update()
		pending = 0
		return nil
	}

	for _, reader := range readers {
		fr := fenestra.NewReader(reader, fenestra.Limits{})

		table := ""
		for {
			frame, err := fr.Next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return err
			}

			switch frame.Kind {
			case fenestra.FrameMeta:
				continue
			case fenestra.FrameTable:
				table = frame.Table
				continue
			case fenestra.FrameRecord:
				tbl, ok := byName[table]
				if !ok {
					// Unknown table name: a deploy-ordering bug, not skippable
					// data (see the stream format spec).
					return fmt.Errorf("unknown snapshot table %q", table)
				}
				kept, err := tbl.RestoreEntity(txn, frame.Key, frame.Value, bounds)
				if err != nil {
					return fmt.Errorf("restoring %s record: %w", table, err)
				}
				if !kept {
					continue
				}
				pending++
				if pending >= restoreTxnChunk {
					if err := commit(); err != nil {
						return err
					}
				}
			}
		}
	}

	if err := txn.Commit(); err != nil {
		return err
	}
	txn = badgerStore.Update() // leaves a clean txn for the deferred Discard

	return badgerStore.Flatten()
}
