Part 10 complete

Error: Db file is not a whole number of pages. Corrupt file.

in pager_open:
    gives error because (file_length % PAGE_SIZE != 0)
        PAGE_SIZE = 4096
        file_length = lseek(file, 0, SEEK_END) = 586