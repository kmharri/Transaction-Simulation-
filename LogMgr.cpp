#include "LogMgr.h"
#include <sstream>
#include <queue>

using namespace std;

///////////////////  LogMgr  ///////////////////

/*
* Find the LSN of the most recent log record for this TX.
* If there is no previous log record for this TX, return
* the null LSN.
*/
int LogMgr::getLastLSN(int txnum) {
	auto lsn = tx_table.find(txnum); //Get iterator for lastLSN if exists
	if (lsn != tx_table.end()) { //If exists reutrn LSN
		return lsn->second.lastLSN;
	}
	return NULL_LSN; //If not return null
}

/*
* Update the TX table to reflect the LSN of the most recent
* log entry for this transaction.
*/
void LogMgr::setLastLSN(int txnum, int lsn) {
	tx_table[txnum].lastLSN = lsn;
}

StorageEngine* se;

/*
* Force log records up to and including the one with the
* maxLSN to disk. Don't forget to remove them from the
* logtail once they're written!
*/
void LogMgr::flushLogTail(int maxLSN) {
	if (maxLSN >= 0) {
		unsigned i = 0;
		while ((i < logtail.size()) && (logtail[i]->getLSN() <= maxLSN)) {
			//Convert to string and flush log entry to disk
			se->updateLog(logtail[i]->toString());
			i++;
		}
		//Remove records from logtail
		logtail.erase(logtail.begin(), logtail.begin() + i);
	}
}

/*
* Run the analysis phase of ARIES.
*/
void LogMgr::analyze(vector <LogRecord*> log) {
	//get chkpt location
	unsigned chkpt = 0;
	if (se->get_master() != NULL_LSN) { //A chkpt exist
		for (unsigned i = log.size() - 1; i > 0; i--) {
			if (log[i]->getLSN() == se->get_master()) {
				chkpt = i;
				break;
			}
		}
		chkpt++; //Goto end_chkpt
		//Initialize TX Table and DP-Table
		ChkptLogRecord* cpr = dynamic_cast<ChkptLogRecord*> (log[chkpt]);
		if (cpr) { //We have instances of TX and DP Tables
			dirty_page_table = cpr->getDirtyPageTable();
			tx_table = cpr->getTxTable();
		}
	}

	//scan log from most recent chkpt
	while (chkpt < log.size()) {
		int lsn = log[chkpt]->getLSN(); //get LSN
		int txID = log[chkpt]->getTxID(); //get TxID
		int type = log[chkpt]->getType(); //get Type
		//TX not in TX table - Add it
		//Change status to C
		if (type == COMMIT) {
			tx_table[txID].status = C;
			setLastLSN(txID, lsn);
		}
		//remove TX from TX-table
		else if (type == END) {
			tx_table.erase(txID);
		}
		else if (type == ABORT) {
			setLastLSN(txID, lsn);
			tx_table[txID].status = U;
		}
		//If page is affected and not in DP-Table
		//add dirty page to DP-Table
		else if ((log[chkpt]->getType() == UPDATE) ||
			(log[chkpt]->getType() == CLR)) {
			setLastLSN(txID, lsn);
			tx_table[txID].status = U;
			if ((log[chkpt]->getType() == UPDATE)) {
				UpdateLogRecord* DP = dynamic_cast<UpdateLogRecord*>
					(log[chkpt]);
				if (dirty_page_table.find(DP->getPageID()) ==
					dirty_page_table.end()) {
					dirty_page_table[DP->getPageID()] = lsn;
				}
			}
			else { //CLR
				CompensationLogRecord* CLR = dynamic_cast<CompensationLogRecord*>
					(log[chkpt]);
				if (dirty_page_table.find(CLR->getPageID()) ==
					dirty_page_table.end()) {
					dirty_page_table[CLR->getPageID()] = lsn;
				}
			}
		}
		chkpt++;
	}
}
	
/*
* Run the redo phase of ARIES.
* If the StorageEngine stops responding, return false.
* Else when redo phase is complete, return true.
*/
bool LogMgr::redo(vector <LogRecord*> log) {
	//Find samllest recLSN in DP-Table
	if (!dirty_page_table.empty()) {
		int recLSN = dirty_page_table.begin()->second;
		unsigned start = 0;
		for (auto DP = dirty_page_table.begin();
			DP != dirty_page_table.end(); DP++) {
			if (DP->second < recLSN) {
				recLSN = DP->second;
			}
		}
		//Find where to start traversing from in log vector
		for (unsigned i = 0; i < log.size(); i++) {
			if (log[i]->getLSN() == recLSN) {
				start = i;
				break;
			}
		}
		while (start < log.size()) {
			//Is record is redoable:
			//Is the page in the dirty table?
			//Is the DP's recLSN <= current LSN?
			//Is pageLSN < current LSN?
			if ((log[start]->getType() == UPDATE) ||
				(log[start]->getType() == CLR)) {
				int currentLSN = log[start]->getLSN();
				UpdateLogRecord* DP;
				CompensationLogRecord* CLR;
				int pid;
				int offset;
				string after;
				int lsn;
				//Update redo protocol
				if (log[start]->getType() == UPDATE) {
					DP = dynamic_cast<UpdateLogRecord*>
						(log[start]);
					if ((dirty_page_table.find(DP->getPageID()) !=
						dirty_page_table.end()) &&
						(dirty_page_table[DP->getPageID()] <=
						currentLSN)) {
						if (se->getLSN(DP->getPageID()) < currentLSN) {
							pid = DP->getPageID();
							offset = DP->getOffset();
							after = DP->getAfterImage();
							lsn = DP->getLSN();
							//apply update/CLR to page and update PageLSN
							if (!se->pageWrite(pid, offset, after, lsn)) {
								return false;
							}
						}
					}
				}
				else { //CLR redo protocol
					CLR = dynamic_cast<CompensationLogRecord*> (log[start]);
					if ((dirty_page_table.find(CLR->getPageID()) !=
						dirty_page_table.end()) &&
						(dirty_page_table[CLR->getPageID()] <=
						currentLSN)) {
						if (se->getLSN(CLR->getPageID()) < currentLSN) {
							pid = CLR->getPageID();
							offset = CLR->getOffset();
							after = CLR->getAfterImage();
							lsn = CLR->getLSN();
							//apply update/CLR to page and update PageLSN
							if (!se->pageWrite(pid, offset, after, lsn)) {
								return false;
							}
						}
					}
				}
			}
			start++;
		}
	}
	//Remove committed TXs from tx_table and append to logtail
	for (auto entry = tx_table.begin(); entry != tx_table.end();) {
		if (entry->second.status == C) {
			int nextlsn = se->nextLSN();
			int prevlsn = entry->second.lastLSN;
			int txid = entry->first;
			logtail.push_back(new LogRecord(nextlsn, prevlsn, txid, END));
			entry = tx_table.erase(entry);
		}
		else {
			++entry;
		}
	}
	return true;
}

/*
* If no txnum is specified, run the undo phase of ARIES.
* If a txnum is provided, abort that transaction.
* Hint: the logic is very similar for these two tasks!
*/
void LogMgr::undo(vector <LogRecord*> log, int txnum) {
	if (!tx_table.empty()) {
		bool abort_one = false;
		UpdateLogRecord* DP;
		CompensationLogRecord* CLR1;
		int undoNext;
		int tx_id = NULL_TX;
		int page_id;
		int page_offset;
		string after_img;
		int prev_lsn;
		int start = NULL_LSN; //Start a index in log with top LSN
		int size = log.size() - 1;

		//get LSN of logs to Undo
		priority_queue<int> ToUndo;

		//construct undo order
		if (txnum == NULL_TX) { //Undo Phase of AIRES
			for (int i = size; i >= 0; i--) {
				if ((tx_table.find(log[i]->getTxID()) != tx_table.end()) 
					&& ((log[i]->getType() == UPDATE) 
					|| (log[i]->getType() == CLR))) {
					//Add LSN to ToUndo
					ToUndo.push(log[i]->getLSN());
					if (start == NULL_LSN) {
						start = i;
					}
				}
			}
		}
		else { //ABORT
			abort_one = true;
			if (tx_table.find(txnum) != tx_table.end()) {
				for (int i = size; i >= 0; i--) {
					if ((log[i]->getTxID() == txnum) 
						&& ((log[i]->getType() == UPDATE)
						|| (log[i]->getType() == CLR))) {
						//Add LSN to ToUndo
						ToUndo.push(log[i]->getLSN());
						if (start == NULL_LSN) {
							start = i;
						}
					}
				}
			}
			else {
				return; //Specified TX not live
			}
		}

		while (!ToUndo.empty()) {
			if (ToUndo.top() == log[start]->getLSN()) {
				if (log[start]->getType() == UPDATE) {
					DP = dynamic_cast <UpdateLogRecord*>
						(log[start]);
					prev_lsn = DP->getLSN();
					undoNext = DP->getprevLSN();
					tx_id = DP->getTxID();
					page_id = DP->getPageID();
					page_offset = DP->getOffset();
					after_img = DP->getBeforeImage();
					if (abort_one) {
						if (logtail.empty()) {
							prev_lsn = log[log.size() - 1]->getLSN();
						}
						else {
							prev_lsn = logtail[logtail.size() - 1]->getLSN();
						}
					}
					//Create CLR
					int CLR_lsn = se->nextLSN();
					logtail.push_back(new CompensationLogRecord(CLR_lsn,
						prev_lsn, tx_id, page_id, page_offset,
						after_img, undoNext));
					tx_table[tx_id].lastLSN = CLR_lsn; //Update tx_table

					if (!se->pageWrite(DP->getPageID(), DP->getOffset(),
						DP->getBeforeImage(), DP->getLSN())) { //Revert Page
						return;
					}
					prev_lsn = logtail[logtail.size() - 1]->getLSN();
				}
				else { //TXtype = CLR
					CLR1 = dynamic_cast <CompensationLogRecord*>
						(log[start]);
					undoNext = CLR1->getUndoNextLSN();
					prev_lsn = CLR1->getLSN();
				}
				if (undoNext == NULL_LSN) {
					//Create end log record and remove TX from TX_table
					logtail.push_back(new LogRecord(se->nextLSN(),
						prev_lsn, tx_id, END));
					tx_table.erase(tx_id);
					if (abort_one) { //TX was specified
						return;
					}
				}
				ToUndo.pop();
			}
			start--;
		}
	}
}

vector<LogRecord*> LogMgr::stringToLRVector(string logstring) {
	vector<LogRecord*> result; 
	istringstream stream(logstring); 
	string line; 
	while (getline(stream, line)) {
		LogRecord* lr = LogRecord::stringToRecordPtr(line);  
		result.push_back(lr);
	} 
	return result;
}


/*
* Abort the specified transaction.
* Hint: you can use your undo function
*/
void LogMgr::abort(int txid) {
	string disklog = se->getLog();
	vector <LogRecord*> log = stringToLRVector(disklog);
	if (tx_table.find(txid) != tx_table.end()) {
		//Create abort record for trans
		int prev_lsn = tx_table[txid].lastLSN;
		int lsn_in = se->nextLSN();
		logtail.push_back(new LogRecord(lsn_in,
			prev_lsn, txid, ABORT)); //Add abort record to logtail
		setLastLSN(txid, lsn_in);
		log.insert(log.end(), logtail.begin(), logtail.end());
		undo(log, txid); //END record for trans created in undo
	}
}

/*
* Write the begin checkpoint and end checkpoint
*/
void LogMgr::checkpoint() {
	int lsn_in_begin = se->nextLSN(); //Begin_chkpt lsn
	int lsn_in_end = se->nextLSN(); //End_chkpt lsn
	string disk_log = se->getLog();
	vector <LogRecord*> log = stringToLRVector(disk_log);
	int master_record = se->get_master(); //Getlsn last chkpt
	if (logtail.empty()) { //Get prev_lsn_begin from last record in disklog
		if (!disk_log.empty()) { 
			for (int i = log.size() - 1; i >= 0; i--) {
				if (log[i]->getLSN() == master_record) {
					log.erase(log.begin(), log.begin() + i);
					break;
				}
			}//Get lastest chkpt
			analyze(log); //Get TX and DP tables from disklog
		}
	}
	else { //Get prev_lsn_begin from last record in logtail
		//reconstruct DP and TX tables from entire log
		log.insert(log.end(), logtail.begin(), logtail.end());
		for (int i = log.size() - 1; i >= 0; i--) {
			if (log[i]->getLSN() == master_record) {
				log.erase(log.begin(), log.begin() + i);
				break;
			}
		}//Get lastest chkpt
		analyze(log); //Get TX and DP tables from disklog + logtail
	}
	//Create begin/end chkpts, TX_table and DP_Table
	ChkptLogRecord End_chkpt =
		ChkptLogRecord(lsn_in_end, lsn_in_begin, NULL_TX, tx_table, dirty_page_table);
	tx_table = End_chkpt.getTxTable();
	dirty_page_table = End_chkpt.getDirtyPageTable();
	//Store LSN of begin_chkpt to stable storage
	se->store_master(lsn_in_begin);

	//Update logtail
	logtail.push_back(new LogRecord(lsn_in_begin, 
		NULL_LSN, NULL_TX, BEGIN_CKPT));
	logtail.push_back(new ChkptLogRecord(lsn_in_end, 
		lsn_in_begin, NULL_TX, tx_table, dirty_page_table));
	flushLogTail(lsn_in_end); //flush logtail
}

/*
* Commit the specified transaction.
*/
void LogMgr::commit(int txid) {
	if (tx_table.find(txid) != tx_table.end()) {
		int prev_lsn = tx_table[txid].lastLSN;
		int lsn_in = se->nextLSN();
		logtail.push_back(new LogRecord(lsn_in,
			prev_lsn, txid, COMMIT)); //Add COMMIT record to logtail
		flushLogTail(lsn_in); //flush logtail 
		logtail.push_back(new LogRecord(se->nextLSN(),
			lsn_in, txid, END)); //Add END record to logtail
		tx_table.erase(txid);//remove from txtable
	}
}

/*
* A function that StorageEngine will call when it's about to
* write a page to disk.
* Remember, you need to implement write-ahead logging
*/
void LogMgr::pageFlushed(int page_id) {
	//flush logtail up to record with page's lsn
	flushLogTail(se->getLSN(page_id));
	//remove page from DP-table
	dirty_page_table.erase(page_id);
}

/*
* Recover from a crash, given the log from the disk.
*/
void LogMgr::recover(string log) {
	//AIRES
	vector <LogRecord*> disklog = stringToLRVector(log);
	analyze(disklog);
	if (redo(disklog)) {
		undo(disklog);
	}
}

/*
* Logs an update to the database and updates tables if needed.
*/
int LogMgr::write(int txid, int page_id, int offset, string input, string oldtext) {
	int lsn = se->nextLSN();
	int prev_lsn = NULL_LSN;
	auto last = tx_table.find(txid);
	if (last != tx_table.end()) {
		prev_lsn = last->second.lastLSN;
	}
	//Append update to logtail
	logtail.push_back(new UpdateLogRecord(lsn, prev_lsn,
		txid, page_id, offset, oldtext, input));
	//update tx's lasLSN and status
	tx_table[txid].lastLSN = lsn; 
	tx_table[txid].status = U;
	if (dirty_page_table.find(page_id) == dirty_page_table.end()) {
		dirty_page_table[page_id] = lsn; 
	} //1st instance where page became dirty
	return lsn; //return page_lsn for write
}

/*
* Sets this.se to engine.
*/
void LogMgr::setStorageEngine(StorageEngine* engine) {
	this->se = engine;
}
