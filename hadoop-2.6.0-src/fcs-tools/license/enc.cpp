
#include <iostream>
#include <string>

using namespace std;

int main(int argc, char ** argv) {

  string key = "FreeLunchMWF";
  int keysize = 12;

  string input = string(argv[1]);
  string output = "";

  for(int i = 0; i < input.size(); ++i) {
    int offset = i % keysize;

    int a = input[i] - 32;
    int b = key[offset] - 32;
    int c = (a - b + 91) % 91 + 32;
    output += (char)c;
  }
  cout << output;


  // use this algorithm during decryption
  //input = "";
  //for(int i = 0; i < output.size(); ++i) {
  //  int offset = i % keysize;

  //  int a = output[i] - 32;
  //  int b = key[offset] - 32;
  //  int c = (a + b) % 91 + 32;
  //  input += (char)c;
  //}
  //cout << "input: " << input << endl;
}
