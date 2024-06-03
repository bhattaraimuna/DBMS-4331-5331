/*
 * @(#) bt.java   98/03/24
 * Copyright (c) 1998 UW.  All Rights Reserved.
 *         Author: Xiaohu Li (xioahu@cs.wisc.edu).
 *
 */

/*
 *         CSE 4331/5331 B+ Tree Project (Spring 2024)
 *         Instructor: Abhishek Santra
 *
 */


package btree;

import java.io.*;

import diskmgr.*;
import bufmgr.*;
import global.*;
import heap.*;
import btree.*;
/**
 * btfile.java This is the main definition of class BTreeFile, which derives
 * from abstract base class IndexFile. It provides an insert/delete interface.
 */
public class BTreeFile extends IndexFile implements GlobalConst {

	private final static int MAGIC0 = 1989;
	private static int red = 0;

	private final static String lineSep = System.getProperty("line.separator");

	private static FileOutputStream fos;
	private static DataOutputStream trace;

	/**
	 * It causes a structured trace to be written to a file. This output is used
	 * to drive a visualization tool that shows the inner workings of the b-tree
	 * during its operations.
	 *
	 * @param filename
	 *            input parameter. The trace file name
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void traceFilename(String filename) throws IOException {

		fos = new FileOutputStream(filename);
		trace = new DataOutputStream(fos);
	}

	/**
	 * Stop tracing. And close trace file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 */
	public static void destroyTrace() throws IOException {
		if (trace != null)
			trace.close();
		if (fos != null)
			fos.close();
		fos = null;
		trace = null;
	}

	private BTreeHeaderPage headerPage;
	private PageId headerPageId;
	private String dbname;

	/**
	 * Access method to data member.
	 * 
	 * @return Return a BTreeHeaderPage object that is the header page of this
	 *         btree file.
	 */
	public BTreeHeaderPage getHeaderPage() {
		return headerPage;
	}

	private PageId get_file_entry(String filename) throws GetFileEntryException {
		try {
			return SystemDefs.JavabaseDB.get_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new GetFileEntryException(e, "");
		}
	}

	private Page pinPage(PageId pageno) throws PinPageException {
		try {
			Page page = new Page();
			SystemDefs.JavabaseBM.pinPage(pageno, page, false/* Rdisk */);
			return page;
		} catch (Exception e) {
			e.printStackTrace();
			throw new PinPageException(e, "");
		}
	}

	private void add_file_entry(String fileName, PageId pageno)
			throws AddFileEntryException {
		try {
			SystemDefs.JavabaseDB.add_file_entry(fileName, pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new AddFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno) throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, false /* = not DIRTY */);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	private void freePage(PageId pageno) throws FreePageException {
		try {
			SystemDefs.JavabaseBM.freePage(pageno);
		} catch (Exception e) {
			e.printStackTrace();
			throw new FreePageException(e, "");
		}

	}

	private void delete_file_entry(String filename)
			throws DeleteFileEntryException {
		try {
			SystemDefs.JavabaseDB.delete_file_entry(filename);
		} catch (Exception e) {
			e.printStackTrace();
			throw new DeleteFileEntryException(e, "");
		}
	}

	private void unpinPage(PageId pageno, boolean dirty)
			throws UnpinPageException {
		try {
			SystemDefs.JavabaseBM.unpinPage(pageno, dirty);
		} catch (Exception e) {
			e.printStackTrace();
			throw new UnpinPageException(e, "");
		}
	}

	/**
	 * BTreeFile class an index file with given filename should already exist;
	 * this opens it.
	 *
	 * @param filename
	 *            the B+ tree file name. Input parameter.
	 * @exception GetFileEntryException
	 *                can not ger the file from DB
	 * @exception PinPageException
	 *                failed when pin a page
	 * @exception ConstructPageException
	 *                BT page constructor failed
	 */
	public BTreeFile(String filename) throws GetFileEntryException,
			PinPageException, ConstructPageException {

		headerPageId = get_file_entry(filename);

		headerPage = new BTreeHeaderPage(headerPageId);
		dbname = new String(filename);
		/*
		 * 
		 * - headerPageId is the PageId of this BTreeFile's header page; -
		 * headerPage, headerPageId valid and pinned - dbname contains a copy of
		 * the name of the database
		 */
	}

	/**
	 * if index file exists, open it; else create it.
	 *
	 * @param filename
	 *            file name. Input parameter.
	 * @param keytype
	 *            the type of key. Input parameter.
	 * @param keysize
	 *            the maximum size of a key. Input parameter.
	 * @param delete_fashion
	 *            full delete or naive delete. Input parameter. It is either
	 *            DeleteFashion.NAIVE_DELETE or DeleteFashion.FULL_DELETE.
	 * @exception GetFileEntryException
	 *                can not get file
	 * @exception ConstructPageException
	 *                page constructor failed
	 * @exception IOException
	 *                error from lower layer
	 * @exception AddFileEntryException
	 *                can not add file into DB
	 */
	public BTreeFile(String filename, int keytype, int keysize,
			int delete_fashion) throws GetFileEntryException,
			ConstructPageException, IOException, AddFileEntryException {

		headerPageId = get_file_entry(filename);
		if (headerPageId == null) // file not exist
		{
			headerPage = new BTreeHeaderPage();
			headerPageId = headerPage.getPageId();
			add_file_entry(filename, headerPageId);
			headerPage.set_magic0(MAGIC0);
			headerPage.set_rootId(new PageId(INVALID_PAGE));
			headerPage.set_keyType((short) keytype);
			headerPage.set_maxKeySize(keysize);
			headerPage.set_deleteFashion(delete_fashion);
			headerPage.setType(NodeType.BTHEAD);
		} else {
			headerPage = new BTreeHeaderPage(headerPageId);
		}

		dbname = new String(filename);

	}

	/**
	 * Close the B+ tree file. Unpin header page.
	 *
	 * @exception PageUnpinnedException
	 *                error from the lower layer
	 * @exception InvalidFrameNumberException
	 *                error from the lower layer
	 * @exception HashEntryNotFoundException
	 *                error from the lower layer
	 * @exception ReplacerException
	 *                error from the lower layer
	 */
	public void close() throws PageUnpinnedException,
			InvalidFrameNumberException, HashEntryNotFoundException,
			ReplacerException {
		if (headerPage != null) {
			SystemDefs.JavabaseBM.unpinPage(headerPageId, true);
			headerPage = null;
		}
	}

	/**
	 * Destroy entire B+ tree file.
	 *
	 * @exception IOException
	 *                error from the lower layer
	 * @exception IteratorException
	 *                iterator error
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception FreePageException
	 *                error when free a page
	 * @exception DeleteFileEntryException
	 *                failed when delete a file from DM
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                failed when pin a page
	 */
	public void destroyFile() throws IOException, IteratorException,
			UnpinPageException, FreePageException, DeleteFileEntryException,
			ConstructPageException, PinPageException {
		if (headerPage != null) {
			PageId pgId = headerPage.get_rootId();
			if (pgId.pid != INVALID_PAGE)
				_destroyFile(pgId);
			unpinPage(headerPageId);
			freePage(headerPageId);
			delete_file_entry(dbname);
			headerPage = null;
		}
	}

	private void _destroyFile(PageId pageno) throws IOException,
			IteratorException, PinPageException, ConstructPageException,
			UnpinPageException, FreePageException {

		BTSortedPage sortedPage;
		Page page = pinPage(pageno);
		sortedPage = new BTSortedPage(page, headerPage.get_keyType());

		if (sortedPage.getType() == NodeType.INDEX) {
			BTIndexPage indexPage = new BTIndexPage(page,
					headerPage.get_keyType());
			RID rid = new RID();
			PageId childId;
			KeyDataEntry entry;
			for (entry = indexPage.getFirst(rid); entry != null; entry = indexPage
					.getNext(rid)) {
				childId = ((IndexData) (entry.data)).getData();
				_destroyFile(childId);
			}
		} else { // BTLeafPage

			unpinPage(pageno);
			freePage(pageno);
		}

	}

	private void updateHeader(PageId newRoot) throws IOException,
			PinPageException, UnpinPageException {

		BTreeHeaderPage header;
		PageId old_data;

		header = new BTreeHeaderPage(pinPage(headerPageId));

		old_data = headerPage.get_rootId();
		header.set_rootId(newRoot);

		// clock in dirty bit to bm so our dtor needn't have to worry about it
		unpinPage(headerPageId, true /* = DIRTY */);

		// ASSERTIONS:
		// - headerPage, headerPageId valid, pinned and marked as dirty

	}

	/**
	 * insert record with the given key and rid
	 *
	 * @param key
	 *            the key of the record. Input parameter.
	 * @param rid
	 *            the rid of the record. Input parameter.
	 * @exception KeyTooLongException
	 *                key size exceeds the max keysize.
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IOException
	 *                error from the lower layer
	 * @exception LeafInsertRecException
	 *                insert error in leaf page
	 * @exception IndexInsertRecException
	 *                insert error in index page
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception NodeNotMatchException
	 *                node not match index page nor leaf page
	 * @exception ConvertException
	 *                error when convert between revord and byte array
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error when search
	 * @exception IteratorException
	 *                iterator error
	 * @exception LeafDeleteException
	 *                error when delete in leaf page
	 * @exception InsertException
	 *                error when insert in index page
	 */

	
	public void insert(KeyClass key, RID rid) throws KeyTooLongException,
			KeyNotMatchException, LeafInsertRecException,
			IndexInsertRecException, ConstructPageException,
			UnpinPageException, PinPageException, NodeNotMatchException,
			ConvertException, DeleteRecException, IndexSearchException,
			IteratorException, LeafDeleteException, InsertException,
			IOException

	{	
		
		//Declare function variables
		KeyDataEntry header;
		BTLeafPage newLeafPage = null;
		BTIndexPage newIndexPage = null;

		//Check for key length 
		//Assertions: if the key length is greater than the max key size set in header
		//				throw error and stop else continue
		if (BT.getKeyLength(key) > headerPage.get_maxKeySize()){
			throw new KeyTooLongException(null, "");
		}

		//Assertions : if key is IntegerKey and header key attribute type declared not Integer
		//             throw error else let the program
		if (key instanceof IntegerKey){
				
			//Assertios: Check for header page being not valid
			//			 if not vaild create a new (Leaf) Page
			//           and insert the entry else call _insert() 
			if (getHeaderPage().get_rootId().pid == INVALID_PAGE){

				//Create a new (Leaf) Page
				newLeafPage = new BTLeafPage(getHeaderPage().get_keyType());
					
				//pin the page
				pinPage(newLeafPage.getCurPage()); // class function returns Page 

				//Set next and previous page pointers
				newLeafPage.setNextPage(new PageId(INVALID_PAGE)); // inherited from HFPage Class non-return type function
				newLeafPage.setPrevPage(new PageId(INVALID_PAGE)); // inherited from HFPage Class non return type fucntion

				//insert the record
				newLeafPage.insertRecord(key,rid); // BTLeafPage class function  returns rid of the inserted record as RID

				// unpin the page and set the dirty bit as changes have been made
				unpinPage(newLeafPage.getCurPage(), true); // class function non return type function 

				//change the header to point to the new page
				updateHeader(newLeafPage.getCurPage());// getCurPage() inherited from HFPage Class return page no as PageId		
			}

			else {
				// fucntion call to _insert class fucntion to insert record
				header = _insert(key,rid,getHeaderPage().get_rootId()); // class function return KeyDataEntry

				// Assertion : if the retured value to the header is not null that means the split
				//			   moved up till the root node and root was split. need to create a new
				//			   root node and manage pointers.
				if (header != null){

					//Create a new (Index) Page
					newIndexPage = new BTIndexPage(getHeaderPage().get_keyType());

					//pin the page
					pinPage(newIndexPage.getCurPage()); //class function returns Page
					// Insert in to the new index node and set previous page to the old root
					newIndexPage.insertKey(header.key,((IndexData)(header.data)).getData()); // BTIndexPage class fuction returns RID of the inserted key
					newIndexPage.setPrevPage(getHeaderPage().get_rootId()); // inherited from HFPage Class non return type fucntion
					//unpin the page and set the dirty bit as changes have been made
					unpinPage(newIndexPage.getCurPage(), true); //class function non return type function 
					//change the header to point to the new page
					updateHeader(newIndexPage.getCurPage()); // getCurPage() inherited from HFPage Class return page no as PageId	
				}
			}
		}
		//Assertions: if any other key Atrribute type throw error
		else {
			throw new KeyNotMatchException(null, "");
		}
	}

	private KeyDataEntry _insert(KeyClass key, RID rid, PageId currentPageId)
			throws PinPageException, IOException, ConstructPageException,
			LeafDeleteException, ConstructPageException, DeleteRecException,
			IndexSearchException, UnpinPageException, LeafInsertRecException,
			ConvertException, IteratorException, IndexInsertRecException,
			KeyNotMatchException, NodeNotMatchException, InsertException

	{
		//Decaler function variables 
		BTSortedPage currentPage = null;
		BTLeafPage leafPage = null;
		BTLeafPage newLeafPage = null;
		BTLeafPage rightPage = null;
		KeyDataEntry finalEntry = null;
		BTIndexPage indexPage = null;
		BTIndexPage newIndexPage = null;
		int slotCount;
		int i;
		KeyDataEntry keyData = null;
		KeyDataEntry curEntry = null;
		KeyDataEntry entry = null;
		//pin the page
		pinPage(currentPageId); //class function return Page
		//Create an instance of BTSortedPage as we dont know what type of node (leaf or index) we are at 

		currentPage = new BTSortedPage(currentPageId,getHeaderPage().get_keyType());

		
		// Assertions: Check for the node type
		//				if Leaf: check for space and insert or split
		//				if index: recursively travse till leaf node is found
		// 				else: throw NodeNotMatch error
		if (currentPage.getType() == NodeType.LEAF){
			// Create an instance of BTLeafPage to access the fucntion of BTLeafPage

			leafPage = new BTLeafPage(currentPageId,getHeaderPage().get_keyType());


			//Assertions: if the avaiable space in the leafpage is more than the current key length
			// 				insert the key in to the leaf page else split.

			// BT.getKeyDataLength(key,node_type) BT class function returns space required for the key to be stored in the type of node
			if (leafPage.available_space() >= BT.getKeyDataLength(key,NodeType.LEAF)){	//avaiable_space() inhertied form HFPage returns space as int
				
				// insert the record
				leafPage.insertRecord(key,rid); // BTLeafPage class function  returns rid of the inserted record as RID

				// unpin the page and set the dirty bit as changes are made
				unpinPage(leafPage.getCurPage(),true); // class function non return type
				// As there was space in the node and there was no need to split the parent node so return null
				return null;
			}
			// Space was not enough perform split
			else {
				// Create a new (Leaf) Page 

				newLeafPage = new BTLeafPage(getHeaderPage().get_keyType());

				//Set next and previous page pointers
				newLeafPage.setNextPage(leafPage.getNextPage()); // inherited from HFPage Class non-return type function
				newLeafPage.setPrevPage(leafPage.getCurPage()); // inherited from HFPage Class non-return type function
				leafPage.setNextPage(newLeafPage.getCurPage()); // inherited from HFPage Class non-return type function
				// set the reverse pointer of the page that was to the right of the leafPage 
				if (newLeafPage.getNextPage().pid != INVALID_PAGE){
					//pin the page
					pinPage(newLeafPage.getNextPage()); // class function returns Page
					// Create the instance of BTLeafPage to access BTLeafPage fucntions

					rightPage = new BTLeafPage(newLeafPage.getNextPage(), getHeaderPage().get_keyType());

					// set the previous page pointer to the newLeafPage
					rightPage.setPrevPage(newLeafPage.getCurPage()); // inherited from HFPage Class non-return type function
					// unpin the right page and set the dirty bit as changes have been made
					unpinPage(rightPage.getCurPage(),true); // class function non return type.
				}
				// get the slot count to transfer all the data from leafPage to newLeafPage
				slotCount = leafPage.getSlotCnt(); // inherited from HPPage class returns slot count as short d-type
				// fetch first recored from leafPage and insert it into newLeafPage and delete from leafPage do for all records
				for (i = 0; i<slotCount;i++){
					// get first record
					keyData= leafPage.getCurrent(new  RID());// BTLeafPage class funciton return KeyDataEntry
					newLeafPage.insertRecord(keyData.key,((LeafData)(keyData.data)).getData());// BTLeafPage class function returns KeyDataEntry
					leafPage.deleteSortedRecord(new RID()); // inherited from BTSortedPage

				}
				//Assertions: leafPage is empty newLeafPage is completely filled
				//			  now transfer 1st half entires to leafPage
				for (i = 0; newLeafPage.available_space()<leafPage.available_space();i++){ //avaiable_space() inhertied form HFPage returns space as int
					//get the first entry in the newLeafPage
					keyData = newLeafPage.getCurrent(new RID()); //BTLeafPage class function return KeyDataEntry
					finalEntry = keyData;	// save the final key to compare to add new value in the nodes
					// insert the record
					leafPage.insertRecord(keyData.key,((LeafData)(keyData.data)).getData()); //BTLeafPage class function return RID of the inserted record
					// delete the record from the newLeafPage
					newLeafPage.deleteSortedRecord(new RID());// inherited from the BTSortedPage class non return type

				}
			
				//Assertion: check the current key with final key entry if it is less than add to the left side
				//			  if it is more than or equal to add to the rightside 
				if(BT.keyCompare(finalEntry.key,key)<0){// BT.keyCompare() compare the key and return +ve, -ve or 0 integer based on key values

				newLeafPage.insertRecord(key,rid); // insert the record in the newLeafNode as the key is greater then the final key value


				}
				else if (BT.keyCompare(finalEntry.key,key)>=0){ // BT.keyCompare() compare the key and return +ve, -ve or 0 integer based on key values
				leafPage.insertRecord(key,rid); // insert the record in the newLeafNode as the key i less then or equal the final key value

				}
				else {
					System.out.println("Invalid key"); // key doesnt belong to either node
					return null;
				}
				// unpin both pages
				unpinPage(leafPage.getCurPage(),true); // class function non return type
				unpinPage(newLeafPage.getCurPage(),true); // class fucntion non return type
				return new KeyDataEntry(newLeafPage.getFirst(new RID()).key,newLeafPage.getCurPage());	// copy up the first value of the newLeafPage	
			}
		}
		// None leaf node was found we need to recursivly travse
		else if (currentPage.getType() == NodeType.INDEX){ // check if the page is and index page or not

			// initialize the BTIndexPage to access its fucntion
			indexPage = new BTIndexPage(currentPage, getHeaderPage().get_keyType());

			
			//unpin the page dont set the dirty bit as no changes have been made.
			unpinPage(currentPageId);
			// recursively call _insert(key,rid,PageId) untill you reach leaf node.
			curEntry = _insert(key,rid,indexPage.getPageNoByKey(key)); // BTIndexPage class function returns PageId
			
			// Assertion: if curEntry is null no split happend and no changes are needed so return null
			if (curEntry == null){
				return null;
			}
			// split occured and need to add the value is to be added
			// pin the page that needs to change
			pinPage(currentPageId); // class fucntion returns PageId
			//create the instance of the BTIndexPage to access its function
			indexPage = new BTIndexPage(currentPageId,getHeaderPage().get_keyType());
			//Assertion : check for space if you can insert in the index node if not then split
			if (indexPage.available_space() >= BT.getKeyDataLength(key,NodeType.INDEX)){	
				//Space is enough to add an entry so just insert				
				indexPage.insertKey(curEntry.key,((IndexData)curEntry.data).getData()); // BTIndexPage class function returns RID of the inserted record
				//unpin the page and set the dirt bit
				unpinPage(currentPageId, true); // class function non return type
				return null; // return null as no split happened 
			}
			// we dont have enough space need to split
			else {
				finalEntry = null;
				// create a new BTIndexPage to split the entries
				newIndexPage = new BTIndexPage(getHeaderPage().get_keyType());
				pinPage(newIndexPage.getCurPage());
				pinPage(indexPage.getCurPage());
				// get the number of slots 
				slotCount = indexPage.getSlotCnt(); // inherited from HFPage class returns number of slots in short data type
				// iterate till all the entries from the indexPage are moved to the newIndexPage
				for (i = 0; i<slotCount;i++){
					// get the first record
					keyData = indexPage.getFirst(new RID()); // BTIndexPage class fucntion return first record in the indexPage
					// insert the record in the newIndexPage
					newIndexPage.insertKey(keyData.key,((IndexData)keyData.data).getData()); // BTIndexPage class function
					// delete the record from the indexPage
					indexPage.deleteSortedRecord(new RID()); // inherited from the BTSortedPage class

				}
				// try to make eqaul split with half the values in each node
				for (i = 0; indexPage.available_space()>newIndexPage.available_space();i++){ //avaiable_space() inhertied form HFPage returns space as int
					// get the first record
					keyData = newIndexPage.getFirst(new RID()); // BTIndexPage class fucntion return first record in the indexPage
					// copy the entry 
					finalEntry = keyData;
					// insert the record in the indexPage
					indexPage.insertKey(keyData.key,((IndexData)keyData.data).getData()); // BTIndexPage class function
					// delete the record from the newIndexPage
					newIndexPage.deleteSortedRecord(new RID()); // inherited from the BTSortedPage class

				}
				// check for even split if not move the last entry from indexPage to newIndexPage
				if (indexPage.available_space()<newIndexPage.available_space()){
					// insert the record in the newIndexPage

					newIndexPage.insertKey(finalEntry.key, ((IndexData)finalEntry.data).getData()); // BTIndexPage class function

					// delete the record in the indexPage
					indexPage.deleteSortedRecord(new RID(indexPage.getCurPage(),(int)indexPage.getSlotCnt()-1)); // inherited from the BTSortedPage class
				}
				// get the first entry of the newIndexPage node for comparison
				entry = newIndexPage.getFirst(new RID());  // BTIndexPage class function returns first data record as KeyDataEntry
				// if the first entry is greater than or equal to the current key that needs to inserted add to newIndexPage
				if(BT.keyCompare(entry.key,curEntry.key)<=0){ // BT.keyCompare() compare the key and return +ve, -ve or 0 integer based on key values
					newIndexPage.insertKey(curEntry.key,((IndexData)curEntry.data).getData()); // BTIndexPage class fucntion returns RID of the inserted record
				}
				// if the first entry is less than the current key that needs to inserted add to newIndexPage
				else if (BT.keyCompare(entry.key,curEntry.key)>0){ // BT.keyCompare() compare the key and return +ve, -ve or 0 integer based on key values
					indexPage.insertKey(curEntry.key,((IndexData)curEntry.data).getData()); // BTIndexPage class fucntion returns RID of the inserted record
				}
				else {
					System.out.println("Invalid key"); // recored doesnt belong to either node
					return null;
				}

				//unpin currentIndexPage and set the dirty bit as changes are made
				unpinPage(indexPage.getCurPage(), true); // class fucntion non return type
				// need to copy up the first value and remove it form the newIndexPage
				curEntry = newIndexPage.getFirst(new RID()); // BTIndexPage class function returns KeyDataEntry of first record
				newIndexPage.setPrevPage( ((IndexData)curEntry.data).getData()); // set the previous page to approriate pointer
	  			newIndexPage.deleteSortedRecord(new RID());
				//unpin newIndexPage and set the dirty bit as changes are made
				unpinPage(newIndexPage.getCurPage(), true); // class fucntion non return type
				return new KeyDataEntry(curEntry.key,newIndexPage.getCurPage()); // return KeyDataEntry to update the parent node
			}
		}
		else{
			// node is neither leaf nor index so need to throw the error
			// System.out.println("Wrong node type");
			throw new  NodeNotMatchException("Wrong node type");
		}
		
	}
	
				
		
	


	



	/**
	 * delete leaf entry given its <key, rid> pair. `rid' is IN the data entry;
	 * it is not the id of the data entry)
	 *
	 * @param key
	 *            the key in pair <key, rid>. Input Parameter.
	 * @param rid
	 *            the rid in pair <key, rid>. Input Parameter.
	 * @return true if deleted. false if no such record.
	 * @exception DeleteFashionException
	 *                neither full delete nor naive delete
	 * @exception LeafRedistributeException
	 *                redistribution error in leaf pages
	 * @exception RedistributeException
	 *                redistribution error in index pages
	 * @exception InsertRecException
	 *                error when insert in index page
	 * @exception KeyNotMatchException
	 *                key is neither integer key nor string key
	 * @exception UnpinPageException
	 *                error when unpin a page
	 * @exception IndexInsertRecException
	 *                error when insert in index page
	 * @exception FreePageException
	 *                error in BT page constructor
	 * @exception RecordNotFoundException
	 *                error delete a record in a BT page
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception IndexFullDeleteException
	 *                fill delete error
	 * @exception LeafDeleteException
	 *                delete error in leaf page
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception DeleteRecException
	 *                error when delete in index page
	 * @exception IndexSearchException
	 *                error in search in index pages
	 * @exception IOException
	 *                error from the lower layer
	 *
	 */
	public boolean Delete(KeyClass key, RID rid) throws DeleteFashionException,
			LeafRedistributeException, RedistributeException,
			InsertRecException, KeyNotMatchException, UnpinPageException,
			IndexInsertRecException, FreePageException,
			RecordNotFoundException, PinPageException,
			IndexFullDeleteException, LeafDeleteException, IteratorException,
			ConstructPageException, DeleteRecException, IndexSearchException,
			IOException {
		if (headerPage.get_deleteFashion() == DeleteFashion.NAIVE_DELETE)
			return NaiveDelete(key, rid);
		else
			throw new DeleteFashionException(null, "");
	}

	/*
	 * findRunStart. Status BTreeFile::findRunStart (const void lo_key, RID
	 * *pstartrid)
	 * 
	 * find left-most occurrence of `lo_key', going all the way left if lo_key
	 * is null.
	 * 
	 * Starting record returned in *pstartrid, on page *pppage, which is pinned.
	 * 
	 * Since we allow duplicates, this must "go left" as described in the text
	 * (for the search algorithm).
	 * 
	 * @param lo_key find left-most occurrence of `lo_key', going all the way
	 * left if lo_key is null.
	 * 
	 * @param startrid it will reurn the first rid =< lo_key
	 * 
	 * @return return a BTLeafPage instance which is pinned. null if no key was
	 * found.
     *
     *  ASantra [1/7/2023]: Modified]
	 */

 

	BTLeafPage findRunStart(KeyClass lo_key, RID startrid) throws IOException,
			IteratorException, KeyNotMatchException, ConstructPageException,
			PinPageException, UnpinPageException {
		BTLeafPage pageLeaf;
		BTIndexPage pageIndex;
		Page page;
		BTSortedPage sortPage;
		PageId pageno;
		PageId curpageno = null; // Iterator
		PageId prevpageno;
		PageId nextpageno;
		RID curRid;
		KeyDataEntry curEntry;

		pageno = headerPage.get_rootId();

		if (pageno.pid == INVALID_PAGE) { // no pages in the BTREE
			pageLeaf = null; // should be handled by
			// startrid =INVALID_PAGEID ; // the caller
			return pageLeaf;
		}

		page = pinPage(pageno);
		sortPage = new BTSortedPage(page, headerPage.get_keyType());

		if (trace != null) {
			trace.writeBytes("VISIT node " + pageno + lineSep);
			trace.flush();
		}

		// ASSERTION
		// - pageno and sortPage is the root of the btree
		// - pageno and sortPage valid and pinned

		while (sortPage.getType() == NodeType.INDEX) {
			pageIndex = new BTIndexPage(page, headerPage.get_keyType());
			prevpageno = pageIndex.getPrevPage();
			curEntry = pageIndex.getFirst(startrid);
			while (curEntry != null && lo_key != null
					&& BT.keyCompare(curEntry.key, lo_key) < 0) {

				prevpageno = ((IndexData) curEntry.data).getData();
				curEntry = pageIndex.getNext(startrid);
			}

			unpinPage(pageno);

			pageno = prevpageno;
			page = pinPage(pageno);
			sortPage = new BTSortedPage(page, headerPage.get_keyType());

			if (trace != null) {
				trace.writeBytes("VISIT node " + pageno + lineSep);
				trace.flush();
			}

		}

		pageLeaf = new BTLeafPage(page, headerPage.get_keyType());

		curEntry = pageLeaf.getFirst(startrid);
		while (curEntry == null) {
			// skip empty leaf pages off to left
			nextpageno = pageLeaf.getNextPage();
			unpinPage(pageno);
			if (nextpageno.pid == INVALID_PAGE) {
				// oops, no more records, so set this scan to indicate this.
				return null;
			}

			pageno = nextpageno;
			pageLeaf = new BTLeafPage(pinPage(pageno), headerPage.get_keyType());
			curEntry = pageLeaf.getFirst(startrid);
		}

		// ASSERTIONS:
		// - curkey, curRid: contain the first record on the
		// current leaf page (curkey its key, cur
		// - pageLeaf, pageno valid and pinned

		if (lo_key == null) {
			return pageLeaf;
			// note that pageno/pageLeaf is still pinned;
			// scan will unpin it when done
		}

		while (BT.keyCompare(curEntry.key, lo_key) < 0) {
			curEntry = pageLeaf.getNext(startrid);
			while (curEntry == null) { // have to go right
				nextpageno = pageLeaf.getNextPage();
				unpinPage(pageno);

				if (nextpageno.pid == INVALID_PAGE) {
					return null;
				}

				pageno = nextpageno;
				pageLeaf = new BTLeafPage(pinPage(pageno),
						headerPage.get_keyType());

				curEntry = pageLeaf.getFirst(startrid);
			}
		}

		return pageLeaf;
	}

	/*
	 * Status BTreeFile::NaiveDelete (const void *key, const RID rid)
	 * 
	 * Remove specified data entry (<key, rid>) from an index.
	 * 
	 * We don't do merging or redistribution, but do allow duplicates.
	 * 
	 * Page containing first occurrence of key `key' is found for us by
	 * findRunStart. We then iterate for (just a few) pages, if necesary, to
	 * find the one containing <key,rid>, which we then delete via
	 * BTLeafPage::delUserRid.
	 */

	private boolean NaiveDelete(KeyClass key, RID rid)
			throws LeafDeleteException, KeyNotMatchException, PinPageException,
			ConstructPageException, IOException, UnpinPageException,
			PinPageException, IndexSearchException, IteratorException {// This function deletes a record including duplicates or 
			//it deletes a range of records. It has a return type boolean but it doesn't matter what it returns as we need this function just to delete the record. 
            boolean check = true;/* A boolean variable to check if the leafPage where we are searching 
			for the record is null or not */
			BTLeafPage leafPage = findRunStart(key,new RID());/* Get the leafPage with the first
			occurence of the key for which the record is to be deleted.
			FindRunStart has a return value of leafPage which is an object of BTLeaf class
			 */

			while (check == true){/* Checks if there are more pages available to check for the record to be deleted*/
				if (leafPage == null){/* If we have checked all the leafPages having the same key as the record to be deleted then leafPage reaches null */
					check = false;/* Make check as false to come out of the loop*/
					System.out.println("No Instance of Record "+key+" was found");
				}
				else {
					pinPage(leafPage.getCurPage());// Pin the page as we are using it to check for the record to be deleted
					check = leafPage.delEntry(new KeyDataEntry(key, rid));// Checks if the record is present on the given leafPage and if it is present 
					//delEntry deletes it and returns true else it will return false as the recordis not presnt

					if (check == true){
						unpinPage(leafPage.getCurPage(),true);// unpin the leafPage as the recorded is deleted and now the leafPage is not needed 
						System.out.println("Instance of Record "+key+" deleted successfully");
					}
					else {
						unpinPage(leafPage.getCurPage(),false);// unpin the leafpage as there was no change no need to set dirty bit
						System.out.println("All Instances of Record "+key+" if existed are now deleted");
					}
						
				}
			}
			
			return true;

	}
	/**
	 * create a scan with given keys Cases: (1) lo_key = null, hi_key = null
	 * scan the whole index (2) lo_key = null, hi_key!= null range scan from min
	 * to the hi_key (3) lo_key!= null, hi_key = null range scan from the lo_key
	 * to max (4) lo_key!= null, hi_key!= null, lo_key = hi_key exact match (
	 * might not unique) (5) lo_key!= null, hi_key!= null, lo_key < hi_key range
	 * scan from lo_key to hi_key
	 *
	 * @param lo_key
	 *            the key where we begin scanning. Input parameter.
	 * @param hi_key
	 *            the key where we stop scanning. Input parameter.
	 * @exception IOException
	 *                error from the lower layer
	 * @exception KeyNotMatchException
	 *                key is not integer key nor string key
	 * @exception IteratorException
	 *                iterator error
	 * @exception ConstructPageException
	 *                error in BT page constructor
	 * @exception PinPageException
	 *                error when pin a page
	 * @exception UnpinPageException
	 *                error when unpin a page
	 */
	public BTFileScan new_scan(KeyClass lo_key, KeyClass hi_key)
			throws IOException, KeyNotMatchException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException

	{
		BTFileScan scan = new BTFileScan();
		if (headerPage.get_rootId().pid == INVALID_PAGE) {
			scan.leafPage = null;
			return scan;
		}

		scan.treeFilename = dbname;
		scan.endkey = hi_key;
		scan.didfirst = false;
		scan.deletedcurrent = false;
		scan.curRid = new RID();
		scan.keyType = headerPage.get_keyType();
		scan.maxKeysize = headerPage.get_maxKeySize();
		scan.bfile = this;

		// this sets up scan at the starting position, ready for iteration
		scan.leafPage = findRunStart(lo_key, scan.curRid);
		return scan;
	}

	void trace_children(PageId id) throws IOException, IteratorException,
			ConstructPageException, PinPageException, UnpinPageException {

		if (trace != null) {

			BTSortedPage sortedPage;
			RID metaRid = new RID();
			PageId childPageId;
			KeyClass key;
			KeyDataEntry entry;
			sortedPage = new BTSortedPage(pinPage(id), headerPage.get_keyType());

			// Now print all the child nodes of the page.
			if (sortedPage.getType() == NodeType.INDEX) {
				BTIndexPage indexPage = new BTIndexPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("INDEX CHILDREN " + id + " nodes" + lineSep);
				trace.writeBytes(" " + indexPage.getPrevPage());
				for (entry = indexPage.getFirst(metaRid); entry != null; entry = indexPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + ((IndexData) entry.data).getData());
				}
			} else if (sortedPage.getType() == NodeType.LEAF) {
				BTLeafPage leafPage = new BTLeafPage(sortedPage,
						headerPage.get_keyType());
				trace.writeBytes("LEAF CHILDREN " + id + " nodes" + lineSep);
				for (entry = leafPage.getFirst(metaRid); entry != null; entry = leafPage
						.getNext(metaRid)) {
					trace.writeBytes("   " + entry.key + " " + entry.data);
				}
			}
			unpinPage(id);
			trace.writeBytes(lineSep);
			trace.flush();
		}

	}

}
