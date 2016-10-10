/*Package mmcb - memory mapped compactable buffer.

This library was developed to support safe data storage in a memory mapped files, that has ability to self compact.



The compactable buffer uses memory mapped file to store addressable entries, it provides based CRUD support with EntryAddress.


Add operation allocates in memory data entry which has provided data plus additional extra space to be used for update in the future determined by extension factor.

Update operation uses entry address and data, if there is enough space data will be written to existing data entry, otherwise it will get migrated.
Migration is the process where existing data entry is flaged as deleted and new data entry with passed in data is added, passed in address pointer is updated.

Remove operation only flags data entry as deleted, compaction will eventually remove it.

Get operation returns DataEntry or EntryReader

*/
package mmcb
