/***************** Transaction class **********************/
/*** Implements methods that handle Begin, Read, Write, ***/
/*** Abort, Commit operations of transactions. These    ***/
/*** methods are passed as parameters to threads        ***/
/*** spawned by Transaction manager class.              ***/
/**********************************************************/

/* Spring 2024: CSE 4331/5331 Project 2 : Tx Manager */

/* Required header files */
#include <stdio.h>
#include <stdlib.h>
#include <sys/signal.h>
#include "zgt_def.h"
#include "zgt_tm.h"
#include "zgt_extern.h"
#include <unistd.h>
#include <iostream>
#include <fstream>
#include <pthread.h>

extern void *start_operation(long, long); // start an op with mutex lock and cond wait
extern void *finish_operation(long);      // finish an op with mutex unlock and con signal

extern void *do_commit_abort_operation(long, char); // commit/abort based on char value
extern void *process_read_write_operation(long, long, int, char);

extern zgt_tm *ZGT_Sh; // Transaction manager object

/* Transaction class constructor */
/* Initializes transaction id and status and thread id */
/* Input: Transaction id, status, thread id */

FILE *logfile;

zgt_tx::zgt_tx(long tid, char Txstatus, char type, pthread_t thrid)
{
  this->lockmode = (char)' '; // default
  this->Txtype = type;        // R = read only, W=Read/Write
  this->sgno = 1;
  this->tid = tid;
  this->obno = -1; // set it to a invalid value
  this->status = Txstatus;
  this->pid = thrid;
  this->head = NULL;
  this->nextr = NULL;
  this->semno = -1; // init to an invalid sem value
}

/* Method used to obtain reference to a transaction node      */
/* Inputs the transaction id. Makes a linear scan over the    */
/* linked list of transaction nodes and returns the reference */
/* of the required node if found. Otherwise returns NULL      */

zgt_tx *get_tx(long tid1)
{
  zgt_tx *txptr, *lastr1;

  if (ZGT_Sh->lastr != NULL)
  {                         // If the list is not empty
    lastr1 = ZGT_Sh->lastr; // Initialize lastr1 to first node's ptr
    for (txptr = lastr1; (txptr != NULL); txptr = txptr->nextr)
      if (txptr->tid == tid1) // if required id is found
        return txptr;
    return (NULL); // if not found in list return NULL
  }
  return (NULL); // if list is empty return NULL
}

/* Method that handles "BeginTx tid" in test file     */
/* Inputs a pointer to transaction id, obj pair as a struct. Creates a new  */
/* transaction node, initializes its data members and */
/* adds it to transaction list */

void *begintx(void *arg)
{
  // intialise a transaction object. Make sure it is
  // done after acquiring the semaphore for the tm and making sure that
  // the operation can proceed using the condition variable. When creating
  // the tx object, set the tx to TR_ACTIVE and obno to -1; there is no
  // semno as yet as none is waiting on this tx.

  struct param *node = (struct param *)arg; // get tid and count
  start_operation(node->tid, node->count);
  zgt_tx *tx = new zgt_tx(node->tid, TR_ACTIVE, node->Txtype, pthread_self()); // Create new tx node

  // Writes the Txtype to the file.

  zgt_p(0); // Lock Tx manager; Add node to transaction list

  tx->nextr = ZGT_Sh->lastr;
  ZGT_Sh->lastr = tx;
  zgt_v(0);                                                                     // Release tx manager
  fprintf(ZGT_Sh->logfile, "T%ld\t\t%c\t\tBeginTx\n", node->tid, node->Txtype); // Write log record and close
  fflush(ZGT_Sh->logfile);
  finish_operation(node->tid);
  pthread_exit(NULL); // thread exit
}

/* Method to handle Readtx action in test file    */
/* Inputs a pointer to structure that contans     */
/* tx id and object no to read. Reads the object  */
/* if the object is not yet present in hash table */
/* or same tx holds a lock on it. Otherwise waits */
/* until the lock is released */

void *readtx(void *arg)
{
  struct param *node = (struct param *)arg; // get tid and objno and count
  start_operation(node->tid, node->count);
  zgt_tx *tx = get_tx(node->tid); // create a new tx node

  /*
    Assertion:
    if tx is null meaning it does not exist
    if not null:
      check for status if status is P perform check for lock and perform read
    else:
      do commit abort
  */
  if (tx != NULL)
  {
    if (tx->status == TR_END)
    {
      do_commit_abort_operation(tx->tid, TR_END);
      zgt_v(0);
      finish_operation(tx->tid);
      pthread_exit(NULL);
    }
    else if (tx->status == TR_ABORT)
    {
      do_commit_abort_operation(tx->tid, TR_ABORT);
      zgt_v(0);
      finish_operation(tx->tid);
      pthread_exit(NULL);
    }
    else if (tx->status == TR_WAIT)
    {
      do_commit_abort_operation(tx->tid, TR_WAIT);
      zgt_v(0);
      finish_operation(tx->tid);
      pthread_exit(NULL);
    }
    else if (tx->status == TR_ACTIVE)
    {

      tx->set_lock(node->tid, 1, node->obno, node->count, 'S');
      zgt_v(0);
      finish_operation(tx->tid);
      pthread_exit(NULL);
    }
    else
    {
      printf("T%ld\t%c \tTxStatus\n", tx->tid, tx->status); // Write log record and close
      printf("T\tInvalid Tx state");
      fflush(ZGT_Sh->logfile);
      printf("Invaid Tx State");
    }
  }
  else
  {
    printf("Trying to read from an invalid Tx");
  }

  return 0;
}

void *writetx(void *arg)
{
  struct param *node = (struct param *)arg;

  start_operation(node->tid, node->count);

  zgt_tx *tx = get_tx(node->tid);
  /*
    Assertion:
    if tx is null meaning it does not exist
    if not null:
      check for status if status is P perform check for lock and perform read
    else:
      do commit abort
  */
  if (tx != NULL)
  {
    if (tx->status == TR_END)
    {
      do_commit_abort_operation(tx->tid, TR_END);
      finish_operation(tx->tid);
      pthread_exit(NULL);
    }
    else if (tx->status == TR_ABORT)
    {
      do_commit_abort_operation(tx->tid, TR_ABORT);
      finish_operation(tx->tid);
      pthread_exit(NULL);
    }
    else if (tx->status == TR_WAIT)
    {
      do_commit_abort_operation(tx->tid, TR_WAIT);
      finish_operation(tx->tid);
      pthread_exit(NULL);
    }
    else if (tx->status == TR_ACTIVE)
    {

      tx->set_lock(node->tid, 1, node->obno, node->count, 'X');
      finish_operation(tx->tid);
      pthread_exit(NULL);
    }
    else
    {
      printf("Invaid Tx State");
    }
  }
  else
  {
    printf("Trying to read from an invalid Tx");
  }

  return 0;
}

void *process_read_write_operation(long tid, long obno, int count, char mode)
{
  struct param *node;
  node->tid = tid;
  node->obno = obno;
  node->count = count;

  start_operation(node->tid, node->count);

  zgt_tx *tx = get_tx(node->tid);
  if (mode == 'r')
  {
    start_operation(node->tid, node->count);

    zgt_tx *tx = get_tx(node->tid);

    if (tx != NULL)
    {
      if (tx->status == TR_END)
      {
        do_commit_abort_operation(tx->tid, TR_END);
        finish_operation(tx->tid);
        pthread_exit(NULL);
      }
      else if (tx->status == TR_ABORT)
      {
        do_commit_abort_operation(tx->tid, TR_ABORT);
        finish_operation(tx->tid);
        pthread_exit(NULL);
      }
      else if (tx->status == TR_WAIT)
      {
        do_commit_abort_operation(tx->tid, TR_WAIT);
        finish_operation(tx->tid);
        pthread_exit(NULL);
      }
      else if (tx->status == TR_ACTIVE)
      {
        tx->set_lock(node->tid, 1, node->obno, node->count, 'S');
        finish_operation(tx->tid);
        pthread_exit(NULL);
      }
      else
      {
        printf("Invaid Tx State");
      }
    }
    else
    {
      printf("Trying to read from an invalid Tx");
    }

    return 0;
  }
  else if (mode == 'w')
  {
    start_operation(node->tid, node->count);

    zgt_tx *tx = get_tx(node->tid);

    if (tx != NULL)
    {
      if (tx->status == TR_END)
      {
        do_commit_abort_operation(tx->tid, TR_ABORT);
        finish_operation(tx->tid);
        pthread_exit(NULL);
      }
      else if (tx->status == TR_ABORT)
      {
        do_commit_abort_operation(tx->tid, TR_ABORT);
        finish_operation(tx->tid);
        pthread_exit(NULL);
      }
      else if (tx->status == TR_WAIT)
      {
        do_commit_abort_operation(tx->tid, TR_WAIT);
        finish_operation(tx->tid);
        pthread_exit(NULL);
      }
      else if (tx->status == TR_ACTIVE)
      {
        tx->set_lock(node->tid, 1, node->obno, node->count, 'S');
        finish_operation(tx->tid);
        pthread_exit(NULL);
      }
      else
      {
        printf("Invaid Tx State");
      }
    }
    else
    {
      printf("Trying to read from an invalid Tx");
    }

    return 0;
  }
  else
  {
    printf("Invalid Mode");
    return 0;
  }
}

void *aborttx(void *arg)
{
  struct param *node = (struct param *)arg;

  start_operation(node->tid, node->count);
  zgt_tx *tx = get_tx(node->tid);
  /*
    Assertion:
    if tx is null meaning it does not exist
    if not null:
      do abort
  */
  if (tx != NULL)
  {
    do_commit_abort_operation(tx->tid, TR_ABORT);
    finish_operation(tx->tid);
    pthread_exit(NULL);
  }
  else
  {
    printf("Invalid Tx");
    pthread_exit(NULL);
  }
}

void *committx(void *arg)
{

  struct param *node = (struct param *)arg;
  start_operation(node->tid, node->count);

  zgt_tx *tx = get_tx(node->tid);
  /*
    Assertion:
    if tx is null meaning it does not exist
    if not null:
      do commit
  */
  if (tx != NULL)
  {
    do_commit_abort_operation(tx->tid, TR_END);
    finish_operation(tx->tid);
    pthread_exit(NULL);
  }
  else
  {

    printf("Invalid Tx");
    pthread_exit(NULL);
  }
}

void *do_commit_abort_operation(long t, char status)
{

  zgt_tx *tx = get_tx(t);
  /*
    Assertion:
    if tx is null meaning it does not exist
    if not null:
      check for status and perform commmit and abort respectivly
  */
  if (tx != NULL)
  {
    if (status == TR_ABORT)
    {

      fprintf(ZGT_Sh->logfile, "T%ld\t\t%c\t\tAbortTx\t", t, tx->Txtype);
      fflush(ZGT_Sh->logfile);
      printf("Abort Tx");
    }
    else if (status == TR_END)
    {

      fprintf(ZGT_Sh->logfile, "T%ld\t\t%c\t\tCommitTx\t", t, tx->Txtype);
      fflush(ZGT_Sh->logfile);
      printf("Commit Tx");
    }
    /*Check semaphones for waiting tx and free all locks and remove the tx*/
    int tx_waiting = tx->semno;
    tx->free_locks();
    tx->remove_tx();
    /*if there are tx waiting for lock wake them up*/
    if (tx_waiting != -1)
    {
      int tx_to_release = zgt_nwait(tx_waiting);
      for (int i = 1; i <= tx_to_release; i++)
      {
        zgt_v(tx_waiting);
      }
    }
  }
  else
  {
    printf("Tx does not exists");
  }
  return 0;
}

int zgt_tx::remove_tx()
{
  // remove the transaction from the TM

  zgt_tx *txptr, *lastr1;
  lastr1 = ZGT_Sh->lastr;
  for (txptr = ZGT_Sh->lastr; txptr != NULL; txptr = txptr->nextr)
  { // scan through list
    if (txptr->tid == this->tid)
    {                               // if correct node is found
      lastr1->nextr = txptr->nextr; // update nextr value; done
                                    // delete this;
      return (0);
    }
    else
      lastr1 = txptr->nextr; // else update prev value
  }
  fprintf(ZGT_Sh->logfile, "Trying to Remove a Tx:%ld that does not exist\n", this->tid);
  fflush(ZGT_Sh->logfile);
  printf("Trying to Remove a Tx:%ld that does not exist\n", this->tid);
  fflush(stdout);
  return (-1);
}

/* this method sets lock on objno1 with lockmode1 for a tx*/

int zgt_tx::set_lock(long tid1, long sgno1, long obno1, int count, char lockmode1)
{
  // if the thread has to wait, block the thread on a semaphore from the
  // sempool in the transaction manager. Set the appropriate parameters in the
  // transaction list if waiting.
  // if successful  return(0); else -1

  zgt_hlink *node, *node_tx, *temp;
  zgt_tx *tx;
  // find the obj in hash table
  zgt_p(0);
  node = ZGT_Ht->find(sgno1, obno1);
  zgt_v(0);
  tx = get_tx(tid1);

  // obj is not in hash table
  // we grant the lock
  if (node == NULL)
  {
    zgt_p(0);
    ZGT_Ht->add(tx, sgno1, obno1, lockmode1);
    perform_read_write_operation(tid1, obno1, lockmode1);
    zgt_v(0);
    return 0;
  }
  // obj in hash table
  else
  {
    // check if the tx has the lock
    zgt_p(0);
    temp = ZGT_Ht->findt(this->tid, sgno1, obno1);
    zgt_v(0);
    // tx has lock perform_read_write_operation
    if (temp != NULL)
    {
      perform_read_write_operation(tid1, obno1, lockmode1);
    }
    // tx may not have the lock
    else
    {

      int wait = zgt_nwait(node->tid);
      // check if lock cannot be granted
      if ((lockmode1 == 'S' && node->lockmode == 'S' && wait > 0) || (lockmode1 == 'X') || (node->lockmode == 'X' && lockmode1 == 'S'))
      {

        tx->obno = obno1;
        tx->lockmode = lockmode1;
        tx->status = TR_WAIT;
        tx->setTx_semno(node->tid, node->tid);

        if (tx->Txtype == 'R')
        {
          fprintf(ZGT_Sh->logfile, "T%ld\t\t%c\t\tReadTx\t\t%ld:%d:%d\t\t\tReadLock\tNotGranted\t %c\n", this->tid, this->Txtype, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
          fflush(ZGT_Sh->logfile);
        }
        else if (tx->Txtype == 'W')
        {
          fprintf(ZGT_Sh->logfile, "T%ld\t\t%c\t\tWriteTx\t\t%ld:%d:%d\t\t\tWriteLock\tNotGranted\t %c\n", this->tid, this->Txtype, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
          fflush(ZGT_Sh->logfile);
        }

        zgt_p(node->tid);
        tx->obno = -1;
        tx->lockmode = lockmode1;
        tx->status = TR_ACTIVE;
        tx->perform_read_write_operation(tid1, obno1, lockmode1);
        zgt_v(node->tid);
      }
      // grant the lock
      else
      {
        perform_read_write_operation(tid1, obno1, lockmode1);
      }
    }
  }
  return 0;
}

int zgt_tx::free_locks()
{

  // this part frees all locks owned by the transaction
  // that is, remove the objects from the hash table
  // and release all Tx's waiting on this Tx

  zgt_hlink *temp = head; // first obj of tx

  for (temp; temp != NULL; temp = temp->nextp)
  { // SCAN Tx obj list

    fprintf(ZGT_Sh->logfile, "%ld : %d, ", temp->obno, ZGT_Sh->objarray[temp->obno]->value);
    fflush(ZGT_Sh->logfile);

    if (ZGT_Ht->remove(this, 1, (long)temp->obno) == 1)
    {
      printf(":::ERROR:node with tid:%ld and onjno:%ld was not found for deleting", this->tid, temp->obno); // Release from hash table
      fflush(stdout);
    }
    else
    {
#ifdef TX_DEBUG
      printf("\n:::Hash node with Tid:%ld, obno:%ld lockmode:%c removed\n",
             temp->tid, temp->obno, temp->lockmode);
      fflush(stdout);
#endif
    }
  }
  fprintf(ZGT_Sh->logfile, "\n");
  fflush(ZGT_Sh->logfile);

  return (0);
}

// CURRENTLY Not USED
// USED to COMMIT
// remove the transaction and free all associate dobjects. For the time being
// this can be used for commit of the transaction.

int zgt_tx::end_tx()
{
  zgt_tx *linktx, *prevp;

  // USED to COMMIT
  // remove the transaction and free all associate dobjects. For the time being
  // this can be used for commit of the transaction.

  linktx = prevp = ZGT_Sh->lastr;

  while (linktx)
  {
    if (linktx->tid == this->tid)
      break;
    prevp = linktx;
    linktx = linktx->nextr;
  }
  if (linktx == NULL)
  {
    printf("\ncannot remove a Tx node; error\n");
    fflush(stdout);
    return (1);
  }
  if (linktx == ZGT_Sh->lastr)
    ZGT_Sh->lastr = linktx->nextr;
  else
  {
    prevp = ZGT_Sh->lastr;
    while (prevp->nextr != linktx)
      prevp = prevp->nextr;
    prevp->nextr = linktx->nextr;
  }
  return 0;
}

// currently not used
int zgt_tx::cleanup()
{
  return (0);
}

// routine to print the tx list
// TX_DEBUG should be defined in the Makefile to print
void zgt_tx::print_tm()
{

  zgt_tx *txptr;

#ifdef TX_DEBUG
  printf("printing the tx  list \n");
  printf("Tid\tTxType\tThrid\t\tobjno\tlock\tstatus\tsemno\n");
  fflush(stdout);
#endif
  txptr = ZGT_Sh->lastr;
  while (txptr != NULL)
  {
#ifdef TX_DEBUG
    printf("%ld\t%c\t%ld\t%ld\t%c\t%c\t%d\n", txptr->tid, txptr->Txtype, txptr->pid, txptr->obno, txptr->lockmode, txptr->status, txptr->semno);
    fflush(stdout);
#endif
    txptr = txptr->nextr;
  }
  fflush(stdout);
}

// need to be called for printing
void zgt_tx::print_wait()
{

  // route for printing for debugging

  printf("\n    SGNO        TxType       OBNO        TID        PID         SEMNO   L\n");
  printf("\n");
}

void zgt_tx::print_lock()
{
  // routine for printing for debugging

  printf("\n    SGNO        OBNO        TID        PID   L\n");
  printf("\n");
}

// routine to perform the actual read/write operation as described the project description
// based  on the lockmode

void zgt_tx::perform_read_write_operation(long tid, long obno, char lockmode)
{

  // +7 lock for write and -4 lock for read and log
  int lock = ZGT_Sh->objarray[obno]->value;
  if (lockmode == 'X')
  {
    ZGT_Sh->objarray[obno]->value = lock + 7;
    fprintf(ZGT_Sh->logfile, "T%ld\t\t%c\t\tWriteTx\t\t%ld:%d:%d\t\t\tWriteLock\tGranted\t\t %c\n", this->tid, this->Txtype, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
    fflush(ZGT_Sh->logfile);
  }

  else if (lockmode == 'S')
  {
    ZGT_Sh->objarray[obno]->value = lock - 4;
    fprintf(ZGT_Sh->logfile, "T%ld\t\t%c\t\tReadTx\t\t%ld:%d:%d\t\t\tReadLock\tGranted\t\t %c\n", this->tid, this->Txtype, obno, ZGT_Sh->objarray[obno]->value, ZGT_Sh->optime[tid], this->status);
    fflush(ZGT_Sh->logfile);
  }
}

// routine that sets the semno in the Tx when another tx waits on it.
// the same number is the same as the tx number on which a Tx is waiting
int zgt_tx::setTx_semno(long tid, int semno)
{
  zgt_tx *txptr;

  txptr = get_tx(tid);
  if (txptr == NULL)
  {
    printf("\n:::ERROR:Txid %ld wants to wait on sem:%d of tid:%ld which does not exist\n", this->tid, semno, tid);
    fflush(stdout);
    exit(1);
  }
  if ((txptr->semno == -1) || (txptr->semno == semno))
  { // just to be safe
    txptr->semno = semno;
    return (0);
  }
  else if (txptr->semno != semno)
  {
#ifdef TX_DEBUG
    printf(":::ERROR Trying to wait on sem:%d, but on Tx:%ld\n", semno, txptr->tid);
    fflush(stdout);
#endif
    exit(1);
  }
  return (0);
}

void *start_operation(long tid, long count)
{

  pthread_mutex_lock(&ZGT_Sh->mutexpool[tid]); // Lock mutex[t] to make other
  // threads of same transaction to wait

  while (ZGT_Sh->condset[tid] != count) // wait if condset[t] is != count
    pthread_cond_wait(&ZGT_Sh->condpool[tid], &ZGT_Sh->mutexpool[tid]);
  return 0;
}

// Otherside of teh start operation;
// signals the conditional broadcast

void *finish_operation(long tid)
{
  ZGT_Sh->condset[tid]--;                         // decr condset[tid] for allowing the next op
  pthread_cond_broadcast(&ZGT_Sh->condpool[tid]); // other waiting threads of same tx
  pthread_mutex_unlock(&ZGT_Sh->mutexpool[tid]);
  return 0;
}
