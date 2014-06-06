Files:

node_elect.py           The code for our node process

get_set.chi             A script which tests get and set commands,
                        with replication but no failures

stop.chi                A script which tests fail-stop failures,
                        killing about half of the nodes and 
                        then testing get and set on the remaining
                        nodes, in addition to new leader elections.
                        The function always returns, returning an
                        error if the key range is not accessible

partition.chi           A script which tests partition tolerance,
                        creating a partition of about half the nodes,
                        showing that they remain internally consistent,
                        and agree to the latest values from the broker
                        when they join. Once again, an error is returned
                        if the key range is not accessible from a 
                        partition.

Usage (you probably already know this):

/* Basic Replication */
python broker.py -e "python node_elect.py" -s get_set.chi
/* Fail-stop tolerance */
python broker.py -e "python node_elect.py" -s stop.chi
/* Partition tolerance */
python broker.py -e "python node_elect.py" -s partition.chi



