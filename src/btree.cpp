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
	outIndexName = idxStr.str();



}


// -----------------------------------------------------------------------------
// BTreeIndex::~BTreeIndex -- destructor
// -----------------------------------------------------------------------------

BTreeIndex::~BTreeIndex() {
	
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
// BTreeIndex::startScan
// -----------------------------------------------------------------------------

void BTreeIndex::startScan(const void* lowValParm,
	const Operator lowOpParm,
	const void* highValParm,
	const Operator highOpParm) {

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