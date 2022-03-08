/**
 * @author See Contributors.txt for code contributors and overview of BadgerDB.
 *
 * @section LICENSE
 * Copyright (c) 2012 Database Group, Computer Sciences Department, University of Wisconsin-Madison.
 */

#include "btree.h"
#include "filescan.h"
#include "exceptions/bad_index_info_exception.h"
#include "exceptions/bad_opcodes_exception.h"
#include "exceptions/bad_scanrange_exception.h"
#include "exceptions/no_such_key_found_exception.h"
#include "exceptions/scan_not_initialized_exception.h"
#include "exceptions/index_scan_completed_exception.h"
#include "exceptions/file_not_found_exception.h"
#include "exceptions/end_of_file_exception.h"


// #define DEBUG

namespace badgerdb {

// -----------------------------------------------------------------------------
// BTreeIndex::BTreeIndex -- Constructor
// -----------------------------------------------------------------------------
BTreeIndex::BTreeIndex(const std::string & relationName, std::string & outIndexName,
											 BufMgr *bufMgrIn, const int attrByteOffset, const Datatype attrType) {

	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	outIndexName = idxStr.str();

	// create new index file if doesn't exist
	bool exist = File::exists(outIndexName);
	BlobFile *indexFile = new BlobFile(outIndexName, !exist);

	// attribute declaration
	this->file = (File*)indexFile;
	this->bufMgr = bufMgrIn;
	this->attributeType = attrType;
	this->attrByteOffset = attrByteOffset;
	this->height = 0;
	this->scanExecuting = false;
	this->currentPageNum = Page::INVALID_NUMBER;
	this->leafOccupancy = (Page::SIZE - sizeof(PageId)) / (sizeof(int) + sizeof(RecordId));
	this->nodeOccupancy = (Page::SIZE - sizeof(int) - sizeof(PageId) ) / (sizeof(int) + sizeof(PageId));

	// if index file exists, read
	if(exist){

		Page *headerPage;
		this->headerPageNum = 1;
		// reads header page
		this->bufMgr->readPage(this->file, this->headerPageNum, headerPage);

		// checks if it matches meta page
		IndexMetaInfo *indexMetaInf = (IndexMetaInfo*)headerPage;
		if(strcmp(indexMetaInf->relationName, relationName.c_str()) != 0 || indexMetaInf->attrByteOffset != this->attrByteOffset || indexMetaInf->attrType != this->attributeType){
				throw BadIndexInfoException("File already exists for the corresponding attribute, but values in metapage(relationName, attribute byte offset, attribute type etc.) do not match with values received through constructor parameters.");
		}

		// updates attribute based on meta page
		this->rootPageNum = indexMetaInf->rootPageNo;
		// unpinning
		this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
		return ;
	}

	// if index file does not exist, alloc
	else {
		Page *headerPage;
		// allocates page
		this->bufMgr->allocPage(this->file, this->headerPageNum, headerPage);

		// set up meta/header page
		IndexMetaInfo *indexMetaInf = (IndexMetaInfo*)headerPage;
		strcpy(indexMetaInf->relationName, relationName.c_str());
		indexMetaInf->attrByteOffset = attrByteOffset;
		indexMetaInf->attrType = attrType;

		// initializes root
		Page *root;
		this->bufMgr->allocPage(this->file, this->rootPageNum, root);
		indexMetaInf->rootPageNo = this->rootPageNum;
		
		LeafNodeInt *rootNode = (LeafNodeInt*)root;
		rootNode->sz = 0;
		rootNode->rightSibPageNo = Page::INVALID_NUMBER;

		// unpinning
		this->bufMgr->unPinPage(this->file, this->headerPageNum, true);
		this->bufMgr->unPinPage(this->file, this->rootPageNum, true); 

		// fill in the tree
		FileScan *scanner = new FileScan(relationName, bufMgr);
		try{
			// iterate through all files till the end
			while(true){
				// get rID
				RecordId rid;
				scanner->scanNext(rid);
				std::string record = scanner->getRecord();

				// initialize key
				int key = *((int*)(record.c_str() + attrByteOffset));
				
				// insert entry
				this->insertEntry(&key, rid);
			}
		}
		catch(EndOfFileException e){};
		delete scanner;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- Destructor
// -----------------------------------------------------------------------------
BTreeIndex::~BTreeIndex() {
	// if index scan has been started
	if (this->scanExecuting){
		this->endScan();
	}
	// ensures dirty pages are written to disk
	this->bufMgr->flushFile(this->file);
	delete this->file;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry -- Insert new entry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
	int _key = *((int*)key);

	std::pair<int, PageId> ret = this->insert(0, this->rootPageNum, _key, rid);
	int newKey = ret.first;
	PageId newPageNo = ret.second;

	if (newKey == -1 && newPageNo == Page::INVALID_NUMBER) return ;

	// allocate new root
	Page *page;
	PageId pageNo;
	this->bufMgr->allocPage(this->file, pageNo, page);

	NonLeafNodeInt *node = (NonLeafNodeInt*)page;
	node->sz = 1;
	node->level = this->height;
	node->keyArray[0] = newKey;
	node->pageNoArray[0] = this->rootPageNum;
	node->pageNoArray[1] = newPageNo;

	// update attrs
	this->height += 1;
	this->rootPageNum = pageNo;

	// update metas
	Page *headerPage;
	this->bufMgr->readPage(this->file, this->headerPageNum, headerPage);
	IndexMetaInfo *metaInfo = (IndexMetaInfo*)headerPage;
	metaInfo->rootPageNo = pageNo;
	this->bufMgr->unPinPage(this->file, this->headerPageNum, true);

	// unpin new root
	this->bufMgr->unPinPage(this->file, pageNo, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::startScan -- Begin a filtered scan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm, const Operator lowOpParm,
													 const void* highValParm, const Operator highOpParm) {

	// Check if operator used to test the high and low range is valid
	if ((lowOpParm != GT && lowOpParm != GTE) || (highOpParm != LT && highOpParm != LTE)) {
		throw BadOpcodesException();
	}

	// Check if lowValue > highValue
	if(*((int*)lowValParm) > *((int*)highValParm)){
		throw BadScanrangeException();
	}

	// Set member variables of BTree
	this->scanExecuting = true;
	
	this->lowValInt = *((int*) lowValParm);
	if(lowOpParm == GT) this->lowValInt += 1;
	this->lowOp = GTE;

	this->highValInt = *((int*) highValParm);
	if(highOpParm == LT) this->highValInt -= 1;
	this->highOp = LTE;

	// Traverse to the leaf node that holds int key <= to searched value
	this->traverseTreeToLeafHelper(this->rootPageNum, lowValParm, this->currentPageNum);

	// Assign currentPageData member variable as currently scanned page
	this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);

	// Index of next entry to be scanned
	LeafNodeInt* currentNode = (LeafNodeInt*)this->currentPageData;
	this->nextEntry = this->lowerBound(currentNode, lowValInt);

	// If the node does not contain first entry, no such key is found
	if (this->nextEntry == currentNode->sz) {
		this->bufMgr->unPinPage(this->file, this->currentPageNum, true);
		this->currentPageNum = Page::INVALID_NUMBER;
		throw NoSuchKeyFoundException();
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext -- Fetch the record id of the next index entry
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) {

	// Check if scan process has been commenced and initialized 
	if(!this->scanExecuting){
		throw ScanNotInitializedException();
	}

	// Check if scan page has ended
	if(this->currentPageNum == Page::INVALID_NUMBER){
		throw IndexScanCompletedException();
	}

	// Cast leaf page to leaf node
	LeafNodeInt* currentNode = (LeafNodeInt*)this->currentPageData;

	int key = currentNode->keyArray[this->nextEntry];

	// Check if range of scan exceeded
	if (key > this->highValInt) {
		this->bufMgr->unPinPage(this->file, this->currentPageNum, true);
		this->currentPageNum = Page::INVALID_NUMBER;
		throw IndexScanCompletedException();
	}

	// Obtain record in page
	outRid = currentNode->ridArray[this->nextEntry];
	this->nextEntry += 1;

	// If exhausted all records, move to sibling
	if (this->nextEntry >= currentNode->sz) {
		this->bufMgr->unPinPage(this->file, this->currentPageNum, true);
		this->currentPageNum = currentNode->rightSibPageNo;
		// If sibling is not empty, reset counter and read new page
		if (this->currentPageNum != Page::INVALID_NUMBER) {
			this->nextEntry = 0;
			this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);
		}
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan -- Terminate the current scan
// -----------------------------------------------------------------------------

void BTreeIndex::endScan() {
	// if scan hasn't been started
	if (!this->scanExecuting){
		throw ScanNotInitializedException();
	}

	// handles if currentPageNum is not invalid
	if (this->currentPageNum != Page::INVALID_NUMBER){
		this->bufMgr->unPinPage(this->file, this->currentPageNum, true);
    this->currentPageNum = Page::INVALID_NUMBER;
	}

	// updates scanExecuting
	this->scanExecuting = false;
}

// -----------------------------------------------------------------------------
// BTreeIndex::lowerBound -- Find the lower bound key
// -----------------------------------------------------------------------------
template <class T>
int BTreeIndex::lowerBound(T *node, int key) {
	for (int i = 0; i < node->sz; i++) {
		if (key <= node->keyArray[i]) return i;
	}
	return node->sz;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insert -- Insert new key and record id into the tree
// -----------------------------------------------------------------------------

std::pair<int, PageId> BTreeIndex::insert(int level, PageId pageNo, int key, RecordId rid) {

	std::pair<int, PageId> passUp;
	
	// read this page
	Page *page;
	this->bufMgr->readPage(this->file, pageNo, page);

	// leaf node
	if (level == this->height) {

		LeafNodeInt *node = (LeafNodeInt*)page;

		// key already exist, replace record id
		int idx = this->lowerBound(node, key);
		if (idx < node->sz && node->keyArray[idx] == key) {
			node->ridArray[idx] = rid;
			passUp.first = -1;
			passUp.second = Page::INVALID_NUMBER;
		}

		// enough space, insert in leaf
		else if (node->sz + 1 <= this->leafOccupancy) {
			this->insertEntryLeaf(node, key, rid);
			passUp.first = -1;
			passUp.second = Page::INVALID_NUMBER;
		}

		// split leaf node
		else {
			int retKey;
			PageId retPageNo;
			this->splitLeafNode(node, key, rid, retKey, retPageNo);

			// pass up middle key and page no
			passUp = {retKey, retPageNo};
		}

	}
	
	// internal node
	else {

		NonLeafNodeInt *node = (NonLeafNodeInt*)page;

		// insert in the children
		int idx = this->lowerBound(node, key);
		int nextPageNo = node->pageNoArray[idx];

		// retrieve copied middle key
		std::pair<int, PageId> ret = this->insert(level + 1, nextPageNo, key, rid);
		int newKey = ret.first;
		PageId newPageNo = ret.second;

		// skip new add
		if (newKey == -1 && newPageNo == Page::INVALID_NUMBER) {
			passUp.first = -1;
			passUp.second = Page::INVALID_NUMBER;
		}

		// enough space, insert in leaf
		else if (node->sz + 1 <= this->nodeOccupancy) {
			this->insertEntryNonLeaf(node, newKey, newPageNo);
			passUp.first = -1;
			passUp.second = Page::INVALID_NUMBER;
		}

		// split leaf node
		else {
			int retKey;
			PageId retPageNo;
			this->splitNonLeafNode(node, key, newPageNo, retKey, retPageNo);

			// pass up middle key and page no
			passUp = {retKey, retPageNo};
		}
	}

	// unpin this page
	this->bufMgr->unPinPage(this->file, pageNo, true);

	return passUp;
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntryLeaf -- Add new entry into the leaf node
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntryLeaf(LeafNodeInt *node, int key, RecordId rid) {

	node->keyArray[node->sz] = key;
	node->ridArray[node->sz] = rid;
	node->sz += 1;
	
	// swap until in place
	int idx = node->sz - 1;
	while (idx > 0 && node->keyArray[idx] < node->keyArray[idx-1]) {
		std::swap(node->keyArray[idx], node->keyArray[idx-1]);
		std::swap(node->ridArray[idx], node->ridArray[idx-1]);
		idx -= 1;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntryNonLeaf -- Add new entry into the internal node
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntryNonLeaf(NonLeafNodeInt *node, int key, PageId pageNo) {

	node->keyArray[node->sz] = key;
	node->pageNoArray[node->sz + 1] = pageNo;
	node->sz += 1;
	
	// swap until in place
	int idx = node->sz - 1;
	while (idx > 0 && node->keyArray[idx] < node->keyArray[idx-1]) {
		std::swap(node->keyArray[idx], node->keyArray[idx-1]);
		std::swap(node->pageNoArray[idx + 1], node->pageNoArray[idx]);
		idx -= 1;
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitLeafNode -- Split the leaf node into two nodes
// -----------------------------------------------------------------------------

void BTreeIndex::splitLeafNode(LeafNodeInt *node, int key, RecordId rid, int &retKey, PageId &retPageNo) {

	// allocate new page
	Page *newPage;
	PageId newPageNo;
	this->bufMgr->allocPage(this->file, newPageNo, newPage);

	// initialize new node
	LeafNodeInt *newNode = (LeafNodeInt*)newPage;
	this->initLeafNode(newNode);

	// redistribute keys
	int mid = (node->sz + 1) / 2;
	if (key < node->keyArray[mid - 1]) mid--;
	int move = 0;
	for (int i = mid; i < node->sz; i++) {
		this->insertEntryLeaf(newNode, node->keyArray[i], node->ridArray[i]);
		move += 1;
	}
	node->sz -= move;

	if (key <= node->keyArray[node->sz - 1]) this->insertEntryLeaf(node, key, rid);
	else this->insertEntryLeaf(newNode, key, rid);

	// copy return key and page no
	retKey = node->keyArray[node->sz - 1];
	retPageNo = newPageNo;

	// set right sibling page no
	newNode->rightSibPageNo = node->rightSibPageNo;
	node->rightSibPageNo = newPageNo;

	// unpin new page
	this->bufMgr->unPinPage(this->file, newPageNo, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::splitNonLeafNode -- Split the internal node into two nodes
// -----------------------------------------------------------------------------

void BTreeIndex::splitNonLeafNode(NonLeafNodeInt *node, int key, PageId pageNo, int &retKey, PageId &retPageNo) {

	// allocate new page
	Page *newPage;
	PageId newPageNo;
	this->bufMgr->allocPage(this->file, newPageNo, newPage);

	// initialize new node
	NonLeafNodeInt *newNode = (NonLeafNodeInt*)newPage;
	this->initNonLeafNode(newNode);

	// redistribute keys
	int mid = (node->sz + 1) / 2;
	if (key < node->keyArray[mid - 1]) mid--;
	int move = 0;
	for (int i = mid; i < node->sz; i++) {
		this->insertEntryNonLeaf(newNode, node->keyArray[i], node->pageNoArray[i]);
		move += 1;
	}
	node->sz -= move;

	if (key <= node->keyArray[node->sz - 1]) this->insertEntryNonLeaf(node, key, pageNo);
	else this->insertEntryNonLeaf(newNode, key, pageNo);

	newNode->pageNoArray[0] = node->pageNoArray[node->sz];
	node->sz -= 1;

	// copy return key and page no
	retKey = node->keyArray[node->sz];
	retPageNo = newPageNo;

	// unpin new page
	this->bufMgr->unPinPage(this->file, newPageNo, true);
}

// -----------------------------------------------------------------------------
// BTreeIndex::initLeafNode -- Initialize new leaf node
// -----------------------------------------------------------------------------

void BTreeIndex::initLeafNode(LeafNodeInt *node) {
	node->sz = 0;
	node->rightSibPageNo = Page::INVALID_NUMBER;
}

// -----------------------------------------------------------------------------
// BTreeIndex::initNonLeafNode -- Initialize new internal node
// -----------------------------------------------------------------------------

void BTreeIndex::initNonLeafNode(NonLeafNodeInt *node) {
	node->sz = 0;
	node->level = Page::INVALID_NUMBER;
}

// -----------------------------------------------------------------------------
// traverseTreeToLeaf helper function -- Find the lower bound leaf
// -----------------------------------------------------------------------------

void BTreeIndex::traverseTreeToLeafHelper(PageId rootPageId, const void* key, PageId &leafPageId){

	// cast key
	int intKey = *((int*) key);

	PageId currentPageId = rootPageId, lastPageId;

	// Tracks the level of tree
	for(int level = 0; level < this->height; level++){

		// Retrieve page instance from pageId
		Page* currentPage;
		this->bufMgr->readPage(this->file, currentPageId, currentPage);

		// Tracks pageId at the previous level to be unpinned after reading
		lastPageId = currentPageId;

		// Cast regular page to a non leaf node
		NonLeafNodeInt *currentNode = (NonLeafNodeInt*) currentPage;
		
		// Retrieve the index where pageId lies
		int index = this->lowerBound(currentNode, intKey);

		// Access pageId from non-leaf node pageId array
		currentPageId = currentNode->pageNoArray[index];

		// Unpins read pageId in the previous BTree level
		this->bufMgr->unPinPage(this->file, lastPageId, true);
	}

	// Return leaf page by reference
	leafPageId = currentPageId;
}

}