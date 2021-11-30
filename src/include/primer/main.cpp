#include <iostream>
#include "p0_starter.h"

int main() {
    // std::cout << "Hello, World!" << std::endl;
    //    std::vector<int> v{1,2,3,4};
    //    bustub::RowMatrix<int> m(2,2);
    //    //std::iota(v.begin(),v.end(),0);
    //    m.FillFrom(v);
    //    std::vector<int> source2 = {1,2,3,4};
    //    auto matrix = std::make_unique<bustub::RowMatrix<int>>(2, 2);
    //    matrix->FillFrom(source2);
    //
    //    return 0;
    using namespace bustub;
    auto matrix0 = std::make_unique<RowMatrix<int>>(3, 3);
    //
        const std::vector<int> source0{1, 4, 2, 5, 2, -1, 0, 3, 1};
        matrix0->FillFrom(source0);
    //
    //    for (int i = 0; i < matrix0->GetRowCount(); i++) {
    //        for (int j = 0; j < matrix0->GetColumnCount(); j++) {
    //            std::cout << source0[i * matrix0->GetColumnCount() + j] << std::endl;
    //            std::cout << matrix0->GetElement(i, j) <<std::endl;
    //        }
    //    }

    auto matrix1 = std::make_unique<RowMatrix<int>>(3, 3);
    const std::vector<int> source1{2, -3, 1, 4, 6, 7, 0, 5, -2};
    matrix1->FillFrom(source1);

    const std::vector<int> expected{3, 1, 3, 9, 8, 6, 0, 8, -1};

    // Perform the addition operation
    auto sum = RowMatrixOperations<int>::Add(matrix0.get(), matrix1.get());

    // Result of addition should have same dimensions as inputs
    std::cout<< sum->GetRowCount() << "cmpred with 3" << std::endl;
    std::cout<< sum->GetColumnCount() << "compared with 3" << std::endl;

    for (int i = 0; i < sum->GetRowCount(); i++) {
        for (int j = 0; j < sum->GetColumnCount(); j++) {
            std::cout<< expected[i * sum->GetColumnCount() + j] <<std::endl;
            std::cout<< sum->GetElement(i, j) << std::endl;
        }
    }
}