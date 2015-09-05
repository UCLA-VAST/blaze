
all:
	make -C ./common
	cd accrdd; mvn package; mvn install
	make -C ./manager/src

examples:
	make -C examples
