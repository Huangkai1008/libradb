<a name="unreleased"></a>
## [Unreleased]


<a name="v0.6.0"></a>
## [v0.6.0] - 2024-08-06
### Bug Fixes
- **disk:** disk space manager unallocated error
- **page:** concurrent issues

### Code Refactoring
- **record:** use linked list to implement record

### Features
- **datastructure:** linkedlist implementation
- **disk:** memory disk space manager implementation
- **disk:** disk manager interface
- **memory:** buffer pool implementation
- **memory:** page replacer interface
- **memory:** lru-k replacer implementation
- **tree:** node unpin
- **tree:** tree delete
- **tree:** use linked list to implement B+ tree index


<a name="v0.5.0"></a>
## [v0.5.0] - 2024-07-28
### Bug Fixes
- **field:** float field value type
- **field:** integer value type check
- **index:** judge whether a page is leaf

### Code Refactoring
- **page:** remove unused implementation
- **table:** export table schema field name

### Features
- **config:** add default page size
- **index:** b+ tree index get/put implementation
- **index:** new inner node
- **index:** use data page to implement b+ tree
- **index:** b+ tree base implementation
- **memory:** buffer manager pin and unpin method
- **page:** data page record id
- **page:** new data page


<a name="v0.4.0"></a>
## [v0.4.0] - 2024-06-01
### Bug Fixes
- github action yml
- github action yml

### Features
- **index:** b+ tree
- **index:** b+ leaf node
- **index:** b+ tree node interface
- **page:** data page record id
- **page:** page header


<a name="v0.3.0"></a>
## [v0.3.0] - 2024-05-05
### Code Refactoring
- lint issues
- **table:** move schema

### Features
- **field:** field value create help function
- **field:** field value implements comparable interface
- **memory:** buffer manager interface
- **page:** page interface
- **table:** table space id
- **typing:** comparable interface
- **util:** find insert index in a leaf page
- **util:** search index in b+ tree


<a name="v0.2.0"></a>
## [v0.2.0] - 2024-02-28
### Features
- **field:** boolean value and float value
- **field:** integer value and variable value
- **field:** variable type length
- **field:** varchar type implementation
- **field:** float type
- **field:** boolean type
- **field:** integer type
- **field:** varchar type
- **field:** nullable field type
- **table:** schema


<a name="v0.1.0"></a>
## v0.1.0 - 2024-01-14
### Features
- **field:** type interface


[Unreleased]: https://github.com/Huangkai1008/libradb/compare/v0.6.0...HEAD
[v0.6.0]: https://github.com/Huangkai1008/libradb/compare/v0.5.0...v0.6.0
[v0.5.0]: https://github.com/Huangkai1008/libradb/compare/v0.4.0...v0.5.0
[v0.4.0]: https://github.com/Huangkai1008/libradb/compare/v0.3.0...v0.4.0
[v0.3.0]: https://github.com/Huangkai1008/libradb/compare/v0.2.0...v0.3.0
[v0.2.0]: https://github.com/Huangkai1008/libradb/compare/v0.1.0...v0.2.0
