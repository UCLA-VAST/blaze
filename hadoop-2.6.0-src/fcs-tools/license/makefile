TARGET = license_gen

all:
	g++ enc.cpp -o ${TARGET}

clean:
	rm -rf ${TARGET}

run: ${TARGET}
	mkdir -p ${HADOOP_HOME}/fcs
	./${TARGET} 'October 31, 2016' > fcs_conf.dat
	cp fcs_conf.dat ${HADOOP_HOME}/fcs
 
