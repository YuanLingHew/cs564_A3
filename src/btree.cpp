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

BTreeIndex::BTreeIndex(
	const std::string & relationName,
	std::string & outIndexName,
	BufMgr *bufMgrIn,
	const int attrByteOffset,
	const Datatype attrType) {

	std::ostringstream idxStr;
	idxStr << relationName << '.' << attrByteOffset;
	std::string outIndexName = idxStr.str();

	// create new index file if doesn't exist
    bool exist = File::exists(outIndexName);
    BlobFile *indexFile = new BlobFile(outIndexName, !exist);

    // attribute declaration
    this->file = (File*) indexFile;
    this->bufMgr = bufMgrIn;
    this->attributeType = attrType;
    this->attrByteOffset = attrByteOffset;
	this->leafOccupancy = (Page::SIZE - sizeof(PageId)) / (sizeof(int) + sizeof(RecordId));
    this->nodeOccupancy = (Page::SIZE - sizeof(int) - sizeof(PageId) ) / (sizeof(int) + sizeof(PageId));
    this->scanExecuting = false;
    this->currentPageNum = Page::INVALID_NUMBER;
	this->height = 1;

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
		indexMetaInf->rootPageNo = this->rootPageNum;

		// initializes root
		Page *root;
		this->bufMgr->allocPage(this->file, this->rootPageNum, root);
		LeafNodeInt *rootNode = (LeafNodeInt*)root;
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
	}

}

// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
	// ensures dirty pages are written to disk
	this->bufMgr->flushFile(this->file);

	// if index scan has been started
	if (this->scanExecuting){
		this->endScan();
	}
}

// -----------------------------------------------------------------------------
// BTreeIndex::insertEntry
// -----------------------------------------------------------------------------

void BTreeIndex::insertEntry(const void *key, const RecordId rid) {
	int _key = *(int*)_key;
	this->insert(this->rootPageNum, _key, rid);

	// // get path to leaf
	// std::vector<PageId> searchPath;
	// search(key, searchPath);
	// int length = (int)searchPath.size();

	// // if the key is already  existed, replace its record id
	// if (updateExistingKey(searchPath[length - 1], key, rid)) return ;

	// // insert in btree
	// for (int i = length - 2; i >= 0; i--) {
	// 	int pageNo = searchPath[i];
	// 	Page *page;
	// 	this->bufMgr->readPage(this->file, pageNo, page);
		
	// }

}

// -----------------------------------------------------------------------------
// traverseTreeToLeaf helper function
// -----------------------------------------------------------------------------
void traverseTreeToLeafHelper(PageId rootPageId, const void* key, PageId &leafPageId){

	// cast key
	int intKey = *((int*) key);

	PageId currentPageId = rootPageId, lastPageId;

	// Tracks the level of tree
	for(int level = 1; level < this->height; level++){

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


// -----------------------------------------------------------------------------
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
	const Operator lowOpParm,
	const void* highValParm,
	const Operator highOpParm) {

	// Check if operator used to test the high and low range is valid
    if(lowOpParm != GT && lowOpParm != GTE && highOpParm != LT && highOpParm != LTE) {
        throw BadOpcodesException();
    }

    // Check if lowValue > highValue
    if(*((int*)lowValParm) > *((int*)highValParm)){
        throw BadScanrangeException();
    }

    // Set member variables of BTree
    this->scanExecuting = true;
    
    this->lowValInt = *((int*) lowValParm);
    if(lowOpParm == GT){

        this->lowValInt += 1;
    }
    this->lowOp = GTE;

    this->highValInt = *((int*) highValParm);
    if(highOpParm == LT){
        this->highValInt -= 1;
    }
    this->highOp = LTE;

    // Index of next entry to be scanned
    this->nextEntry = 0;

    // Traverse to the leaf node that holds int key <= to searched value
	traverseTreeToLeafHelper(this->rootPageId, lowValParm, this->currentPageNum);

	// Assign currentPageData member variable as currently scanned page
	this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);

	// Cast leaf page to leaf node
	LeafNodeInt* currentNode = (LeafNodeInt*)this->currentPageData;

	// Iterate through the singly linked list of the leaf node level
	while(true){

		// Check if index of BTree node has reached the end element
		if(this->nextEntry >= currentNode->sz){
			
			// Unpin previously read node page
			this->bufMgr->unPinPage(this->file, this->currentPageNum, true);

			// Checks if neighboring pageId node has no more key to traverse
			if(currentNode->rightSibPageNo == Page::INVALID_NUMBER){
				throw NoSuchKeyFoundException();
			}
			// Retrieve neighboring pageId using singly-linked pointer
			this->currentPageNum = currentNode->rightSibPageNo;

			// Change index of page to 0 to reflect first element in node
			this->nextEntry = 0;
			
			// Reads new subsequent leaf node page
			this->bufMgr->readPage(this->file, this->currentPageNum, this->currentPageData);

		// Checks if element scanned is smaller than required range
		}else if(this->lowOpInt > currentNode->keyArray[this->nextEntry]){

			// Move to subsequent elements, as key is still not in range
			this->nextEntry++;

		// Check if range of scan exceeded
		}else if(this->highValInt < currentNode->keyArray[this->nextEntry]){
			
			// Terminate search 
			this->bufMgr->unPinPage(this->file, this->currentPageNum, true);
			this->currentPageNum = Page::INVALID_NUMBER;
			throw NoSuchKeyFoundException();

		}else{
			// Scan initialization is valid to commence
			return;
		}


	}


}

// -----------------------------------------------------------------------------
// BTreeIndex::scanNext
// -----------------------------------------------------------------------------

void BTreeIndex::scanNext(RecordId& outRid) {

}

// -----------------------------------------------------------------------------
// BTreeIndex::endScan
// -----------------------------------------------------------------------------
//
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

template <class T>
int BTreeIndex::lowerBound(T *node, int key) {
	for (int i = 0; i < node->sz; i++) {
		if (key <= node->keyArray[i]) return i;
	}
	return node->sz;
}

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
			passUp = {-1, -1};
		}

		// enough space, insert in leaf
		if (node->sz + 1 <= this->leafOccupancy) {
			this->insertEntryLeaf(node, key, rid);
			passUp = {-1, -1};
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
		auto &[newKey, newPageNo] = this->insert(level + 1, nextPageNo, key, rid);

		// skip new add
		if (newKey == -1 && newPageNo == -1) passUp = {-1, -1};

		// enough space, insert in leaf
		if (node->sz + 1 <= this->nodeOccupancy) {
			insertEntryNonLeaf(node, newKey, newPageNo);
			passUp = {-1, -1};
		}

		// split leaf node
		else {
			int retKey;
			PageId retPageNo;
			splitNonLeafNode(node, key, newPageNo, retKey, retPageNo);

			// pass up middle key and page no
			passUp = {retKey, retPageNo};
		}
	}

	// unpin this page
	this->bufMgr->unPinPage(this->file, pageNo, true);

	return passUp;
}

void BTreeIndex::insertEntryLeaf(LeafNodeInt *node, int key, RecordId rid) {

	node->keyArray[node->sz] = key;
	node->ridArray[node->sz] = rid;
	node->sz += 1;
	
	// swap until in place
	int idx = node->sz - 1;
	while (idx > 0 && node->keyArray[idx] < node->keyArray[idx-1]) {
		swap(node->keyArray[idx], node->keyArray[idx-1]);
		swap(node->ridArray[idx], node->ridArray[idx-1]);
	}
}

void BTreeIndex::insertEntryNonLeaf(NonLeafNodeInt *node, int key, PageId pageNo) {

	node->keyArray[node->sz] = key;
	node->pageNoArray[node->sz + 1] = pageNo;
	node->sz += 1;
	
	// swap until in place
	int idx = node->sz - 1;
	while (idx > 0 && node->keyArray[idx] < node->keyArray[idx-1]) {
		swap(node->keyArray[idx], node->keyArray[idx-1]);
		swap(node->pageNoArray[idx + 1], node->pageNoArray[idx]);
	}
}

void BTreeIndex::splitLeafNode(LeafNodeInt *node, int key, RecordId rid, int &retKey, PageId &retPageNo) {

	// allocate new page
	int newPageNo;
	Page *newPage;
	this->bufMgr->allocPage(this->file, newPageNo, newPage);

	// initialize new node
	LeafNodeInt *newNode = (LeafNodeInt*)newPage;
	this->initLeafNode(newNode);

	// redistribute keys
	int mid = (node->sz + 1) / 2;
	if (key < node->keyArray[mid - 1]) mid--;
	for (int i = mid; i < node->sz; i++) {
		this->insertEntryLeaf(newNode, node->keyArray[i], node->ridArray[i]);
		node->sz -= 1;
	}
	if (node->sz <= newNode->sz) this->insertEntryLeaf(node, key, rid);
	else this->insertEntryLeaf(newNode, key, rid);

	// copy return key and page no
	retKey = node->keyArray[node->sz - 1];
	retPageNo = newPageNo;

	// set right sibling page no
	newNode->rightSibPageNo = node->rightSibPageNo;
	node->rightSibPageNo = newNode;

	// unpin new page
	this->bufMgr->unPinPage(this->file, newPageNo, true);
}

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
	for (int i = mid; i < node->sz; i++) {
		this->insertEntryNonLeaf(newNode, node->keyArray[i], node->pageNoArray[i]);
		node->sz -= 1;
	}
	if (node->sz <= newNode->sz) this->insertEntryNonLeaf(node, key, pageNo);
	else this->insertEntryNonLeaf(newNode, key, pageNo);

	newNode->pageNoArray[0] = node->pageNoArray[node->sz];
	node->sz -= 1;

	// copy return key and page no
	retKey = node->keyArray[node->sz];
	retPageNo = newPageNo;

	// unpin new page
	this->bufMgr->unPinPage(this->file, newPageNo, true);
}

}