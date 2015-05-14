#**********************
#*
#* Progam Name: MP1. Membership Protocol.
#*
#* Current file: run.sh
#* About this file: Submission shell script.
#* 
#***********************
#!/bin/sh
wget https://spark-public.s3.amazonaws.com/cloudcomputing2/assignments/mp2_assignment.zip || { echo 'ERROR ... Please install wget' ; exit 1; }
unzip mp2_assignment.zip || { echo 'ERROR ... Zip file not found' ; exit 1; }
cd mp2_assignment
cp ../MP2Node.* .
cp ../MP1Node.* .
make clean > /dev/null 2>&1
make > /dev/null 2>&1 
case $1 in
	0) echo "CREATE test"
	./Application testcases/create.conf > /dev/null 2>&1;;
	1) echo "DELETE test" 
	./Application testcases/delete.conf > /dev/null 2>&1;;
	2) echo "READ test"
	./Application testcases/read.conf > /dev/null 2>&1;;
  3) echo "UPDATE test"
	./Application testcases/update.conf > /dev/null 2>&1;;
	*) echo "Please enter a valid option";;
esac
cp dbg.log ../
cd ..
rm -rf mp2_assignment
rm mp2_assignment.zip
