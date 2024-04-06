## About
This is a mini project I've done while reading the [Designing Data Intensive Applications](https://www.amazon.co.uk/Designing-Data-Intensive-Applications-Reliable-Maintainable/dp/1449373321) books Data Storage section.
As the name implies, this is a Key-Value database. The stinkyDB is quite a basic implementation of some of they key concepts of a key value database such as memtable as a Red-Black tree and disk storage as an LSM-Tree made out of SSTables.
This one also has a pre-cache layer in front of the memtable which is a map for faster access to the most recent data over triversing the Red-Black tree.
