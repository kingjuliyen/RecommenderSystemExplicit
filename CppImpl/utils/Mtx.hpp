#ifndef MTX_HPP
#define MTX_HPP
// Created: 27 Dec 2016

#include <iostream>
#include <cstdio>
#include <string>
#include <iostream>
#include "Utils.hpp"

using namespace std;

class Mtx {
private:
  //long long rows, cols;
  FLT_T * dat;
  size_t mtxDataSz;


  FLT_T * allocateMem() {
//    cout << " rows " << rows << " cols " << cols
//         << " sizeof(FLT_T) " << sizeof(FLT_T) << "\n";
//    cout << "Mtx allocateMem() Allocating "
//         << (sizeof(FLT_T) * rows * cols) << " bytes \n";
    mtxDataSz = sizeof(FLT_T) * rows * cols;
    return (FLT_T *) malloc (mtxDataSz);
  }

public:
  long long rows, cols;
  Mtx(INT_T r, INT_T c) : rows(r), cols(c), dat(0) {
    dat = allocateMem();
  }
  Mtx(INT_T r, INT_T c, FLT_T initVal) : rows(r), cols(c), dat(0) {
    dat = allocateMem();
    for(INT_T r=0; r<rows; r++) {
      for(INT_T c=0; c<cols; c++) {
	       set(r,c,initVal);
      }
    }
  }
  Mtx(const char* filePath): rows(0), cols(0), dat(0){ readMtxFromFileSystem(filePath); }
  ~Mtx() {
    // cout << "~Mtx\n";
    if(dat) {
      //cout << "~Mtx free("<< dat <<")\n";
      free(dat);
      dat = 0;
    }
  }

  FLT_T get(INT_T r, INT_T c) {
    return dat[(r*cols) + c];
  }

  FLT_T get(INT_T r, INT_T c) const {
    return dat[(r*cols) + c];
  }

  void set(INT_T r, INT_T c, FLT_T v) {
    dat[(r*cols) + c] = v;
  }
  void print() {
    for(INT_T r=0; r<rows; r++) {
      for(INT_T c=0; c<cols; c++) {
	       cout << " " << get(r,c);
      }
      cout << "\n";
    }
  }

  void copy(Mtx *dst) {
    for(INT_T r=0; r<rows; r++) {
      for(INT_T c=0; c<cols; c++) {
	       dst->set(r,c, get(r, c));
      }
    }
  }

  void writeHeader(FILE *fp)
  {
    size_t rowSz = fwrite(&rows, sizeof(rows), 1, fp);
    size_t colSz = fwrite(&cols, sizeof(cols), 1, fp);
//    cout << " rowSz " << rowSz << " sizeof(rows) "  << sizeof(rows) << "\n";
//    cout << " colSz " << colSz << " sizeof(cols) "  << sizeof(cols) << "\n";
    if((rowSz + colSz) != 2)
      throw("Mtx::writeHeader (rowSz + colSz) != sizeof(rows) + sizeof(cols)");
  }

  void writeMatrixData(FILE *fp)
  {
    size_t mtxSz = fwrite(dat, mtxDataSz, 1, fp);
    //cout << " mtxSz " << mtxSz << "\n";
    if(mtxSz!= 1)
      throw("Mtx::writeMatrixData");
  }

  void writeMtxToFileSystem(const char* filePath)
  {
    FILE * fp = fopen(filePath, "wb");

    if(!fp)
      throw("Mtx::writeMtxToFileSystem if(!fp)");
    writeHeader(fp);
    writeMatrixData(fp);
    fclose(fp);
  }

  void readMtxFromFileSystem(const char* filePath)
  {
    FILE * fp = fopen(filePath, "rb");
    if(!fp)
      throw("Mtx::readMtxFromFileSystem if(!fp)");

    size_t rws = fread(&rows, sizeof(rows), 1, fp);
    size_t cls = fread(&cols, sizeof(cols), 1, fp);
    if(rws+cls != 2 || (rows + cols) <=0)
      throw("Mtx::readMtxFromFileSystem if(rws+cls != 2 || (rows + cols) <=0)");

    dat = allocateMem();
    if(!dat)
      throw("Mtx::readMtxFromFileSystem if(!dat)");

    size_t mtxSz = fread(dat, mtxDataSz, 1, fp);
    //cout << " mtxSz " << mtxSz << "\n";
    if(mtxSz!= 1)
      throw("Mtx::readMtxFromFileSystem");
  }

  bool compare(const Mtx &m2) const
  {
    const Mtx &m1 = *this;
    if( m1.rows != m2.rows || m1.cols != m1.cols) {
      cout << " Mtx::compare if( m1.rows != m2.rows || m1.cols != m1.cols)\n";
      return false;
    }
    for(INT_T r=0; r<rows; r++) {
      for(INT_T c=0; c<cols; c++) {
         if(m1.get(r,c) != m2.get(r,c)) {
           cout << " Mtx::compare if(m1.get(r,c) != m2.get(r,c)) \n"
            << " r:"<<r<<" c:"<< c
            << " m1.get(r,c) " << m1.get(r,c)
            << " m2.get(r,c) " << m2.get(r,c) << "\n";

          return false;
        }
      }
    }
    return true;
  }

};

#endif  // MTX_HPP

#ifdef ENABLE_MAIN
// g++ Mtx.hpp -o /tmp/t1 -DINT_T=int -DFLT_T=float -DENABLE_MAIN
void test1() {
  cout << "test1\n";
  Mtx m(4,3);

  m.set(0,0,1); m.set(0,1,2); m.set(0,2,3);
  m.set(1,0,4); m.set(1,1,5); m.set(1,2,6);
  m.set(2,0,7); m.set(2,1,8); m.set(2,2,9);
  m.set(3,0,10); m.set(3,1,11); m.set(3,2,12);

  m.print();
}

void test2() {
  Mtx m(4,3,0.8485);
  m.print();
}

void test3() {
  Mtx m(4,3);

  m.set(0,0,1); m.set(0,1,2); m.set(0,2,3);
  m.set(1,0,4); m.set(1,1,5); m.set(1,2,6);
  m.set(2,0,7); m.set(2,1,8); m.set(2,2,9);
  m.set(3,0,10); m.set(3,1,11); m.set(3,2,12);

  //m.print();
  try{
    m.writeMtxToFileSystem("/tmp/m_4_x_3.dat");
    Mtx m3("/tmp/m_4_x_3.dat");
    for(long long r = 0; r < m3.rows; r++) {
      for(long long c = 0; c < m3.cols; c++) {
        cout << " " << m.get(r, c) << " == " << m3.get(r, c) << "\n";
      }
    }
  } catch(const exception &e) {
    cout << " " << e.what() << "\n";
  }
}

int main() {
  // test1();
  // test2();
  test3();
}
#endif
