//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// p0_starter.h
//
// Identification: src/include/primer/p0_starter.h
//
// Copyright (c) 2015-2020, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <memory>
#include <stdexcept>
#include <vector>

#include "common/exception.h"

namespace bustub {

/**
 * The Matrix type defines a common
 * interface for matrix operations.
 */
template <typename T>
class Matrix {
 protected:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new Matrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   *
   */
  Matrix(int rows, int cols) {
      // throw exceptions when paras are negative.
      rows_ = rows;
      cols_ = cols;
      linear_ = new T[rows_ * cols_];
  }

  /** The number of rows in the matrix */
  int rows_ = 0;
  /** The number of columns in the matrix */
  int cols_ = 0;

  /**
   * TODO(P0): Allocate the array in the constructor.
   * TODO(P0): Deallocate the array in the destructor.
   * A flattened array containing the elements of the matrix.
   */
  T *linear_;

 public:
  /** @return The number of rows in the matrix */
  virtual int GetRowCount() const{
      return rows_;
  };

  /** @return The number of columns in the matrix */
  virtual int GetColumnCount() const {
      return cols_;
  };
  /**
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual T GetElement(int i, int j) const {

      if(i < 0 || j < 0 || i >= rows_ || j >= cols_) {
          throw Exception(ExceptionType::OUT_OF_RANGE, "out of range");;
      }
      return linear_[i * cols_ + j ];
      /**
       * rows_ , cols_, 0 - rows_ -1;  i+1, j+1, cols_ * (i) + j+1 - 1;
       * */
      // throw exception.
  };

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  virtual void SetElement(int i, int j, T val) {
      if(i < 0 || j < 0 || i >= rows_ || j >= cols_) {
          throw Exception(ExceptionType::OUT_OF_RANGE, "out of range");
      }
      linear_[i * cols_ + j ] = val;
  };

  /**
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  virtual void FillFrom(const std::vector<T> &source) {
      int sz = rows_ * cols_;
      if(source.size() != static_cast<u_long>(sz) ) {
          throw Exception(ExceptionType::OUT_OF_RANGE, "out fo range");
      }
      for(int i = 0; i < sz; ++i) {
          linear_[i] = source[i];
      }
  };

  /**
   * Destroy a matrix instance.
   * TODO(P0): Add implementation
   */
  virtual ~Matrix() {
      delete[] linear_;
  };
};

/**
 * The RowMatrix type is a concrete matrix implementation.
 * It implements the interface defined by the Matrix type.
 */
template <typename T>
class RowMatrix : public Matrix<T> {
 public:
  /**
   * TODO(P0): Add implementation
   *
   * Construct a new RowMatrix instance.
   * @param rows The number of rows
   * @param cols The number of columns
   */
  RowMatrix(int rows, int cols) : Matrix<T>(rows, cols) {
    data_ = new T* [rows];
    // linear_ = new T [rows * cols] already called in base cstor.
    for(int i = 0; i < rows; ++i) {
        data_ [i] = this->linear_ + i * cols;
    }

  }

  /**
   * TODO(P0): Add implementation
   * @return The number of rows in the matrix
   */
  int GetRowCount() const override { return this->rows_; }

  /**
   * TODO(P0): Add implementation
   * @return The number of columns in the matrix
   */
  int GetColumnCount() const override { return this->cols_; }

  /**
   * TODO(P0): Add implementation
   *
   * Get the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @return The (i,j)th matrix element
   * @throws OUT_OF_RANGE if either index is out of range
   */
  T GetElement(int i, int j) const override {

      if(i < 0 || j < 0 || i >= this->rows_ || j >= this->cols_ ) {
          throw Exception(ExceptionType::OUT_OF_RANGE, "out of range");
      }
        return data_ [i][j];

      //    throw NotImplementedException{"RowMatrix::GetElement() not implemented."};
  }

  /**
   * Set the (i,j)th matrix element.
   *
   * Throw OUT_OF_RANGE if either index is out of range.
   *
   * @param i The row index
   * @param j The column index
   * @param val The value to insert
   * @throws OUT_OF_RANGE if either index is out of range
   */
  void SetElement(int i, int j, T val) override {

      if(i < 0 || j < 0 || i >= this->rows_ || j >= this->cols_ ) {
          throw Exception(ExceptionType::OUT_OF_RANGE, "out of range");
      }

      data_ [i][j] = val;
  }

  /**
   * TODO(P0): Add implementation
   *
   * Fill the elements of the matrix from `source`.
   *
   * Throw OUT_OF_RANGE in the event that `source`
   * does not contain the required number of elements.
   *
   * @param source The source container
   * @throws OUT_OF_RANGE if `source` is incorrect size
   */
  void FillFrom(const std::vector<T> &source) override {
      unsigned long sz = this->rows_ * this->cols_;
      if(source.size() != sz ) {
          throw Exception(ExceptionType::OUT_OF_RANGE, "hh");
      }
      for(int i = 0; i < static_cast<int>(sz); ++i) {
          this->linear_[i] = source[i];
      }

    //throw NotImplementedException{"RowMatrix::FillFrom() not implemented."};
  }

  /**
   * TODO(P0): Add implementation
   *
   * Destroy a RowMatrix instance.
   */
  ~RowMatrix() override {
      delete data_;
  };

 private:
  /**
   * A 2D array containing the elements of the matrix in row-major format.
   *
   * TODO(P0):
   * - Allocate the array of row pointers in the constructor.
   * - Use these pointers to point to corresponding elements of the `linear` array.
   * - Don't forget to deallocate the array in the destructor.
   */
  T ** data_;
};

/**
 * The RowMatrixOperations class defines operations
 * that may be performed on instances of `RowMatrix`.
 */
template <typename T>
class RowMatrixOperations {
 public:
  /**
   * Compute (`matrixA` + `matrixB`) and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix addition
   */
  static std::unique_ptr<RowMatrix<T>> Add(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation

    int RowA = matrixA->GetRowCount();
    int RowB = matrixB->GetRowCount();
    int ColA = matrixA->GetColumnCount();
    int ColB = matrixB->GetColumnCount();
    if ( (RowA != RowB ) || (ColA != ColB) ) {
        return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    auto res = std::unique_ptr<RowMatrix<T>>( new RowMatrix<T>(matrixA->GetRowCount(),matrixA->GetColumnCount()));
    // RowMatrix<T> res();

    for(int i = 0; i < RowA; ++i) {
        for(int j = 0; j < RowB; ++j) {
            T ElementA = matrixA->GetElement(i,j);
            T ElementB = matrixB->GetElement(i,j);
            res->SetElement(i,j,ElementA + ElementB);
        }
    }
    return res;
  }

  /**
   * Compute the matrix multiplication (`matrixA` * `matrixB` and return the result.
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @return The result of matrix multiplication
   */
  static std::unique_ptr<RowMatrix<T>> Multiply(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB) {
    // TODO(P0): Add implementation

      int RowA = matrixA->GetRowCount();
      int RowB = matrixB->GetRowCount();
      int ColA = matrixA->GetColumnCount();
      int ColB = matrixB->GetColumnCount();
      if  (ColA != RowB ) {
          return std::unique_ptr<RowMatrix<T>>(nullptr);
      }

      auto res = std::unique_ptr<RowMatrix<T>>(new RowMatrix<int>(matrixA->GetRowCount(),matrixB->GetColumnCount())) ;
      int temp = 0;
      for(int i = 0; i < RowA; ++i) {
          for(int j = 0; j < ColB; ++j) {
              for(int k = 0; k < ColA; ++k) {
                  temp += matrixA->GetElement(i,k) * matrixB->GetElement(k,j);
              }

              res->SetElement(i,j,temp);
              temp = 0;
          }

      }


      // result(i,j) is calculated from col

    return res;
  }

  /**
   * Simplified General Matrix Multiply operation. Compute (`matrixA` * `matrixB` + `matrixC`).
   * Return `nullptr` if dimensions mismatch for input matrices.
   * @param matrixA Input matrix
   * @param matrixB Input matrix
   * @param matrixC Input matrix
   * @return The result of general matrix multiply
   */
  static std::unique_ptr<RowMatrix<T>> GEMM(const RowMatrix<T> *matrixA, const RowMatrix<T> *matrixB,
                                            const RowMatrix<T> *matrixC) {
    // TODO(P0): Add implementation
    if(matrixA->GetColumnCount() != matrixB->GetRowCount()
    || matrixA->GetRowCount() != matrixC->GetRowCount()
    || matrixB->GetColumnCount() != matrixC->GetColumnCount() ) {
        return std::unique_ptr<RowMatrix<T>>(nullptr);
    }

    auto res = std::unique_ptr<RowMatrix<T>>(Multiply(matrixA,matrixB));
    res = Add(matrixC, res.get());

    return res;

  }
};
}  // namespace bustub
