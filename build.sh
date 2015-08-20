echo "Building Blaze Manager..."
cd manager/src
make clean; make
cd ../../

echo "Building Blaze runtime..."
cd blaze
build/mvn clean package

echo "Done."
