/*
 * make the base class extendable to manage memory block
 * on other memory space (e.g. FPGA device memory)
 *
 */

namespace acc_runtime {
class DataBlock {

public:
  DataBlock(int _tag, int _size):
    tag(_tag), size(_size) {;}

  int getTag() {
    return tag;
  }
  int getSize() {
    return size;
  }

protected:
  int tag;
  int size;
};

template <typename T>
class Block : public DataBlock
{
public:

private:
  T* data;
};
}
