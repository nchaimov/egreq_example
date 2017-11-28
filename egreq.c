#include <stdio.h>
#include <stdlib.h>
#include <mpi.h>

#ifdef DEBUG
#define LOG(...) fprintf(__VA_ARGS__)
#else
#define LOG(...) 
#endif

typedef enum {
    TYPE_NULL,
    TYPE_SEND,
    TYPE_RECV,
    TYPE_WAIT,
    TYPE_COUNT
}nbc_op_type;

static const char * const strops[TYPE_COUNT] = {
    "NULL",
    "SEND",
    "RECV",
    "WAIT"
};


typedef enum {
    NBC_NO_OP,
    NBC_NEXT_OP,
    NBC_WAIT_OP
}stepper;

struct nbc_op{
    nbc_op_type t;
    short trig;
    short done;
    int remote;
    MPI_Comm comm;
    void * buff;
    int tag;
    MPI_Datatype datatype;
    int count;
    MPI_Request request;
};


typedef struct _xMPI_Request
{
    struct nbc_op  op[16];
    int size;
    int current_off;
    MPI_Request * myself;
}xMPI_Request;





int nbc_op_init( struct nbc_op * op,
                 nbc_op_type type,
                 int remote,
                 MPI_Comm comm,
                 MPI_Datatype datatype,
                 int count,
                 void * buff,
                 int tag )
{
    op->trig = 0;
    op->done = 0;
    op->t = type;
    op->remote = remote;
    op->comm = comm;
    op->buff = buff;
    op->datatype = datatype;
    op->count = count;
    op->tag = tag;
}

int nbc_op_wait_init(struct nbc_op * op) {
    op->trig = 0;
    op->done = 0;
    op->t = TYPE_WAIT;
}



int nbc_op_trigger( struct nbc_op * op )
{

    if( op->trig )
        return NBC_NO_OP;

    if( op->t == TYPE_NULL )
    {
        op->trig = 1;
        return NBC_NEXT_OP;
    }

    if( op->t == TYPE_WAIT )
    {
        if( op->done )
        {
            op->trig = 1;
            return NBC_NEXT_OP;
        }
        else {
            return NBC_WAIT_OP;
        }
    }

    switch (op->t) {
        case TYPE_SEND:
            LOG(stderr, "----> SEND %d\n", op->remote);
            MPI_Isend(op->buff, op->count, op->datatype, op->remote, op->tag, op->comm, &op->request);
            break; 
         case TYPE_RECV:
            LOG(stderr, "<---- RECV %d\n", op->remote);
            MPI_Irecv(op->buff, op->count, op->datatype, op->remote, op->tag, op->comm, &op->request);
            break;
        default:
            LOG(stderr, "BAD OP TYPE\n");
            abort();
    }

    op->trig = 1;

    return NBC_NEXT_OP;
}


int nbc_op_test( struct nbc_op * op  )
{
    int rank;
    MPI_Comm_rank( MPI_COMM_WORLD , &rank );

    if( op->t == TYPE_WAIT )
    {
        //LOG(stderr, "IS W\n");
        return 1;
    }

    if( op->t == TYPE_NULL )
    {
        //LOG(stderr, "IS N\n");
        return 1;
    }

    if( op->done )
    {
        //LOG(stderr, "IS D\n");
        return 1;
    }

    int flag=0;
    MPI_Test( &op->request , &flag,  MPI_STATUS_IGNORE );


    if( flag )
    {
        //LOG(stderr, "%d COMPLETED (%s to %d)\n", rank, strops[op->t], op->remote );
        op->done = 1;
    }
    else
    {
        //LOG(stderr, "%d NOT COMPLETED (%s to %d)\n", rank, strops[op->t], op->remote );
    }

    return flag;
}


xMPI_Request * xMPI_Request_new(MPI_Request * parent, int size)
{
    xMPI_Request * ret = malloc( sizeof(xMPI_Request));

    if( !ret )
    {
        perror("malloc");
        abort();
    }

    ret->size = size;
    int i;
    for (i = 0; i < size; ++i) {

        nbc_op_init( &ret->op[i],
                    TYPE_NULL,
                    -1,
                    MPI_COMM_NULL,
                    MPI_DATATYPE_NULL,
                    0,
                    NULL,
                    0 );


        ret->op[i].t = TYPE_NULL;
    }

    ret->myself = parent;
    ret->current_off = 1;

    return ret;
}

/** Extended Generalized Request Interface **/

int xMPI_Request_query_fn( void * pxreq, MPI_Status * status )
{
    xMPI_Request * xreq = (xMPI_Request*) pxreq;
    int flag;

    status->MPI_ERROR = MPI_SUCCESS;

    return MPI_Status_set_elements( status , MPI_CHAR , 1 );
}

/*
 * Returns :
 * 0 -> DONE
 * 1 -> NOT DONE
 *
 */
static inline int xMPI_Request_gen_poll( xMPI_Request *xreq )
{
    int done, i, j;


    int rank;

    MPI_Comm_rank( MPI_COMM_WORLD , &rank );

    int time_to_leave = 0;
    while(1)
    {
        if( time_to_leave )
            break;


        for (i = 0; i < xreq->current_off ; ++i)
        {
            int ret = nbc_op_trigger( &xreq->op[i] ); 
            
            //LOG(stderr, "%d @ %d (%d / %d)\n", rank,  i, xreq->current_off, xreq->size);

            if( ret == NBC_WAIT_OP )
            {
                int wret = 1;

                for (j = 0; j < i; ++j) {
                    int tmp = nbc_op_test( &xreq->op[j] );
                    wret *= tmp;
                    //LOG(stderr, "%d Wait @ %d == %d  !! %d\n", rank,  j, tmp, wret);
                }

                
                if( wret )
                {
                    xreq->op[i].done = 1;

                    //LOG(stderr, "%d STEP %d WAIT DONE\n", rank, i);
                    //xreq->current_off++;
                    time_to_leave = 1;
                }

                break;
            }
            else if( ret == NBC_NEXT_OP )
            {
                //LOG(stderr, "%d TRIG @ %d\n", rank, i);
                xreq->current_off++;
            }

            /* We may complete on a NULL op */
            if( xreq->current_off == xreq->size )
            {
                // //LOG(stderr, "DONE ON NULL\n");
                time_to_leave=1;
                break;
            }
        }

    }

    //LOG(stderr, "%d POLL DONE %d / %d\n", rank,  xreq->current_off, xreq->size );

    if( xreq->current_off == xreq->size )
    {
        //LOG( stderr, "%d DONE %d / %d\n", rank, xreq->current_off, xreq->size);
        return 0;
    }

    return 1;
}



int xMPI_Request_poll_fn( void * pxreq, MPI_Status * status )
{
    int flag;
    xMPI_Request * xreq = (xMPI_Request*) pxreq;

    int not_done = xMPI_Request_gen_poll( xreq );
    
    if( not_done == 0)
    {
        MPI_Grequest_complete(*xreq->myself);
    }

    return MPI_SUCCESS;
}

int xMPI_Request_wait_fn( int cnt, void ** array_of_states, double timeout, MPI_Status * st )
{
    /* Simple implementation */
    int i;

    int completed = 0;
    char _done_array[128] = {0};
    char *done_array = _done_array;
    if( 128 <= cnt )
    {
        done_array = calloc( cnt ,  sizeof(char));
        if( !done_array )
        {
            perror("malloc");
            return MPI_ERR_INTERN;
        }
    }

    int r;
    MPI_Comm_rank( MPI_COMM_WORLD , &r );

    while( completed != cnt )
    {
        for( i = 0 ; i < cnt ; i++ )
        {
            if( done_array[i] )
                continue;

            xMPI_Request * xreq = (xMPI_Request*) array_of_states[i];

            int not_done = xMPI_Request_gen_poll( xreq );
            
            if( !not_done )
            {
                MPI_Grequest_complete(*xreq->myself);
                ////LOG(stderr, "[%d] Completed %d\n", r , i);
                completed++;
                done_array[i] = 1;
            }
        }
    }

    if( done_array != _done_array )
        free( done_array );


    return MPI_SUCCESS;
}

int xMPI_Request_free_fn( void * pxreq )
{
    xMPI_Request * r = (xMPI_Request*)pxreq;
    ////LOG(stderr, "FREEING %p\n", r->myself);
    free( pxreq );
    return MPI_SUCCESS;
}



int xMPI_Request_cancel_fn( void * pxreq, int complete )
{
    if(!complete)
        return MPI_SUCCESS;
    xMPI_Request * xreq = (xMPI_Request*) pxreq;
    return MPI_SUCCESS;
}

static inline int adjust_rank(int rank, int root) {
    if(rank == root) {
        return 0;
    }
    if(rank == 0) {
        return root;
    }
    return rank;
}

static inline void setup_binary_tree(MPI_Comm comm, int root, int * out_rank, int * out_size, int * out_parent, int * out_lc, int * out_rc) {

    int parent, lc, rc;

    int rank, size;
    MPI_Comm_rank( comm , &rank );
    MPI_Comm_size( comm , &size );

    rank = adjust_rank(rank, root);

    parent = (rank+1)/2 -1;
    lc = (rank + 1 )*2 -1;
    rc= (rank + 1)*2;

    if(rank == 0)
        parent = -1;

    if(size <= lc)
        lc = -1;

    if(size <= rc)
        rc = -1;

    *out_rank = rank;
    *out_size = size;
    *out_parent = adjust_rank(parent, root);
    *out_lc = adjust_rank(lc, root);
    *out_rc = adjust_rank(rc, root);
}

static inline int start_request(xMPI_Request * xreq, MPI_Request * request) {
    return MPIX_Grequest_start( xMPI_Request_query_fn,
                                xMPI_Request_free_fn,
                                xMPI_Request_cancel_fn,
                                xMPI_Request_poll_fn,
                                xMPI_Request_wait_fn,
                                xreq,
                                request);
}

int MPI_Ixbcast(void* buffer, int count, MPI_Datatype datatype, int root, MPI_Comm comm, MPI_Request *request) { 
    // This is more or less the same as the barrier, except that we start from the root
    // of the tree instead of the leaves, and the tree can have an arbitrary root
    // (which is accomplished by swapping 0 and the user-specified root)
    xMPI_Request * xreq = xMPI_Request_new(request, 9);

    int rank, size, parent, lc, rc;
    setup_binary_tree(comm, root, &rank, &size, &parent, &lc, &rc);

    if(0 <= parent) { 
        // If this node has a parent, receive from it
        nbc_op_init(&xreq->op[0], TYPE_RECV, parent, comm, datatype, count, buffer, 12345);    
        // and wait until I receive from it
        nbc_op_wait_init(&xreq->op[1]);
    }

    char c;
    if(0 <= lc) {
        // If this node has a left child, send the payload to it
        nbc_op_init(&xreq->op[2], TYPE_SEND, lc, comm, datatype, count, buffer, 12345);
        // and receive from lc to hear that the left subtree is complete
        nbc_op_init(&xreq->op[3], TYPE_RECV, lc, comm, MPI_CHAR, 1, &c, 23456);
    }

    if(0 <= rc) {
        // If this node has a right child, send the payload to it
        nbc_op_init(&xreq->op[4], TYPE_SEND, rc, comm, datatype, count, buffer, 12345);
        // and receive from rc to hear that the right subtree is complete
        nbc_op_init(&xreq->op[5], TYPE_RECV, rc, comm, MPI_CHAR, 1, &c, 23456);
    }

    // Wait to receive completion notification from children
    nbc_op_wait_init(&xreq->op[6]);

    // Send completion notification to parent, if I have one
    if(0 <= parent) {
        nbc_op_init(&xreq->op[7], TYPE_SEND, parent, comm, MPI_CHAR, 1, &c, 23456);
    }
    
    // Wait for completion notification to be sent
    nbc_op_wait_init(&xreq->op[8]);

    return start_request(xreq, request);
}

int MPI_Ixbarrier( MPI_Comm comm , MPI_Request * req )
{
    xMPI_Request * xreq = xMPI_Request_new(req, 9);

    ////LOG(stderr, "INIT on %p\n", req);

    int rank, size, parent, lc, rc;
    setup_binary_tree(comm, 0, &rank, &size, &parent, &lc, &rc);

    //LOG(stderr, "P: %d LC : %d RC : %d\n", parent, lc , rc);

    char c;

    if( 0 <= lc ){
      //LOG(stderr, "POST %d RECV from Lc %d\n", rank, lc );
        nbc_op_init( &xreq->op[0], TYPE_RECV, lc, comm, MPI_CHAR, 1, &c, 12345 );
    }


    if( 0 <= rc ) 
    {
        //LOG(stderr, "POST %d RECV from Rc %d\n", rank, rc );
        nbc_op_init( &xreq->op[1], TYPE_RECV, rc, comm, MPI_CHAR, 1, &c, 12345 );
    }

    //LOG(stderr, "POST %d WAIT\n", rank );
    //nbc_op_init( &xreq->op[2], TYPE_WAIT, 0, comm, 0, 0, NULL, 0 );
    nbc_op_wait_init(&xreq->op[2]);


    if( 0 <= parent ) {
        //LOG(stderr, "POST %d SEND to Par %d\n", rank, parent );
        nbc_op_init( &xreq->op[3], TYPE_SEND, parent, comm, MPI_CHAR, 1, &c, 12345 );
        //LOG(stderr, "POST %d RECV from Par %d\n", rank, parent );
        nbc_op_init( &xreq->op[4], TYPE_RECV, parent, comm, MPI_CHAR, 1, &c, 12345 );
    }


    //LOG(stderr, "POST %d WAIT\n", rank );
    //nbc_op_init( &xreq->op[5], TYPE_WAIT, parent, comm, MPI_CHAR, 1, &c, 12345 );
    nbc_op_wait_init(&xreq->op[5]);


    if( 0 <= lc )
    {
        //LOG(stderr, "POST %d SEND to Lc %d\n", rank, lc );
        nbc_op_init( &xreq->op[6], TYPE_SEND, lc, comm, MPI_CHAR, 1, &c, 12345 );
    }

    if( 0 <= rc )
    {
        //LOG(stderr, "POST %d SEND to Rc %d\n", rank, rc );
        nbc_op_init( &xreq->op[7], TYPE_SEND, rc, comm, MPI_CHAR, 1, &c, 12345 );
    }

    //LOG(stderr, "POST %d WAIT\n", rank );
    //nbc_op_init( &xreq->op[8], TYPE_WAIT, parent, comm, MPI_CHAR, 1, &c, 12345 );
    nbc_op_wait_init(&xreq->op[8]);
 
    return start_request(xreq, req);
}


int do_bcast_test(int root) {
    int rank, size;
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    int * bcast_buf = calloc(100, sizeof(int));
    if(rank == root) {
        fprintf(stderr, "\n\nWill broadcast from rank %d\n", root);
        int i;
        for(i = 0; i < 100; ++i) {
            bcast_buf[i] = i + 1;
        }    
    }
    MPI_Request bcast_req;
    MPI_Ixbcast(bcast_buf, 100, MPI_INT, root, MPI_COMM_WORLD, &bcast_req);
    fprintf(stderr, "HELLO from %d (Before MPI_Ixbcast wait)\n", rank);
    MPI_Wait(&bcast_req, MPI_STATUS_IGNORE);
    fprintf(stderr, "OLLEH from %d (After MPI_Ixbcast wait)\n", rank);

    // Verify bcast
    int i;
    for(i = 0; i < 100; ++i) {
        if(bcast_buf[i] != i + 1) {
            fprintf(stderr, "Validation failed! On rank %d bcast_buf[%d] should have been %d but was %d.\n", rank, i, i+1, bcast_buf[i]);
            MPI_Abort(MPI_COMM_WORLD, 1);
        }
    }

    free(bcast_buf);
}

int main( int argc, char *argv[])
{
    int rank, size;
    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

  

   // if(rank == 1 )
     //   sleep(3);

    MPI_Request req;

    MPI_Ixbarrier( MPI_COMM_WORLD , &req );

    MPI_Barrier( MPI_COMM_WORLD );

    fprintf(stderr, "HELLO from %d (Before MPI_Ixbarrier wait)\n", rank);

    MPI_Wait( &req, MPI_STATUS_IGNORE );

    fprintf(stderr, "OLLEH from %d (After MPI_Ixbarrier wait)\n", rank);
    
    MPI_Barrier( MPI_COMM_WORLD );

    do_bcast_test(0);

    MPI_Barrier(MPI_COMM_WORLD);

    do_bcast_test(1);
    
    MPI_Barrier(MPI_COMM_WORLD);
   
    MPI_Finalize();

    return 0;
}
