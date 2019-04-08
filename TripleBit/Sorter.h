/*
 * Sorter.h
 * Create on: 2011-3-15
 * 	  Author: liupu
 */
#ifndef SORTER_H
#define SORTER_H

class TempFile;

/// Sort a temporary file
class Sorter {
   public:
   /// Sort a file
   static void sort(TempFile& in,TempFile& out,const char* (*skip)(const char*),int (*compare)(const char*,const char*),bool eliminateDuplicates=false);
};
#endif /*SOTER_H*/
