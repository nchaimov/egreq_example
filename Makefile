MPICC=mpicc

all : egreq

egreq : egreq.c
	$(MPICC) -g -o $@ $^

run : egreq
	mpirun -np 2 ./egreq

clean:
	rm -fr egreq egreqmpc 

#MPC Specific
MPCCC=mpc_cc

egreqmpc : egreq.c
	$(MPCCC) -DMPC -o $@ $^

runmpc : egreqmpc
	mpcrun -n=2 ./egreqmpc
