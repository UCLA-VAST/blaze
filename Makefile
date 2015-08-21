
all:
	make -C ./common
	cd blaze; mvn package
	make -C ./manager/src
