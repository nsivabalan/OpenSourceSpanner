echo $*
comm="ps aux"
echo $comm
awk  '{print "run command '"$*"' at " $1}{system("ssh -o StrictHostKeyChecking=no -i ubuntu@" $1 " \"'"$*"'\" ")}' servers
