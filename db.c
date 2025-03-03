# include <stdio.h>
# include <stdlib.h>
# include <string.h>
# include <stdbool.h>
# include <stdint.h>
# include <io.h>
# include <windows.h>
# include <fcntl.h>
# include <errno.h>

# define COLUMN_USERNAME_SIZE 32
# define COLUMN_EMAIL_SIZE 255

# define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)
# define ID_SIZE size_of_attribute(Row, id)
# define USERNAME_SIZE size_of_attribute(Row, username)
# define EMAIL_SIZE size_of_attribute(Row, email)
# define ROW_SIZE (ID_SIZE + USERNAME_SIZE + EMAIL_SIZE)

# define ID_OFFSET 0
# define USERNAME_OFFSET (ID_OFFSET + ID_SIZE)
# define EMAIL_OFFSET (USERNAME_OFFSET + USERNAME_SIZE)
# define PAGE_SIZE 4096
# define TABLE_MAX_PAGES 100 // Change: 400?
# define S_IWUSR 0200
# define S_IRUSR 0400
#define open _open
#define close _close
#define lseek _lseek

// common node header layout
# define NODE_TYPE_SIZE sizeof(uint8_t)
# define NODE_TYPE_OFFSET 0
# define IS_ROOT_SIZE sizeof(uint8_t)
# define IS_ROOT_OFFSET NODE_TYPE_SIZE
# define PARENT_POINTER_SIZE sizeof(uint32_t)
# define PARENT_POINTER_OFFSET (IS_ROOT_OFFSET + IS_ROOT_SIZE)
# define COMMON_NODE_HEADER_SIZE (NODE_TYPE_SIZE + IS_ROOT_SIZE + PARENT_POINTER_SIZE)

// leaf node header layout
# define LEAF_NODE_NUM_CELLS_SIZE sizeof(uint32_t)
# define LEAF_NODE_NUM_CELLS_OFFSET COMMON_NODE_HEADER_SIZE
# define LEAF_NODE_NEXT_LEAF_SIZE sizeof(uint32_t)
# define LEAF_NODE_NEXT_LEAF_OFFSET (LEAF_NODE_NUM_CELLS_OFFSET + LEAF_NODE_NUM_CELLS_SIZE)
# define LEAF_NODE_HEADER_SIZE (COMMON_NODE_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE + LEAF_NODE_NEXT_LEAF_SIZE)

// leaf node body layout
# define LEAF_NODE_KEY_SIZE sizeof(uint32_t)
# define LEAF_NODE_KEY_OFFSET 0
# define LEAF_NODE_VALUE_SIZE ROW_SIZE
# define LEAF_NODE_VALUE_OFFSET (LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE)
# define LEAF_NODE_CELL_SIZE (LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE)
# define LEAF_NODE_SPACE_FOR_CELLS (PAGE_SIZE - LEAF_NODE_HEADER_SIZE)
# define LEAF_NODE_MAX_CELLS (LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE)

// split the leaf node
# define LEAF_NODE_RIGHT_SPLIT_COUNT ((LEAF_NODE_MAX_CELLS + 1) / 2)
# define LEAF_NODE_LEFT_SPLIT_COUNT (LEAF_NODE_MAX_CELLS + 1 - LEAF_NODE_RIGHT_SPLIT_COUNT)

// internal node header layout
# define INTERNAL_NODE_NUM_KEYS_SIZE sizeof(uint32_t)
# define INTERNAL_NODE_NUM_KEYS_OFFSET COMMON_NODE_HEADER_SIZE
# define INTERNAL_NODE_RIGHT_CHILD_SIZE sizeof(uint32_t)
# define INTERNAL_NODE_RIGHT_CHILD_OFFSET (INTERNAL_NODE_NUM_KEYS_OFFSET + INTERNAL_NODE_NUM_KEYS_SIZE)
# define INTERNAL_NODE_HEADER_SIZE (COMMON_NODE_HEADER_SIZE + INTERNAL_NODE_NUM_KEYS_SIZE + INTERNAL_NODE_RIGHT_CHILD_SIZE)

// internal node body layout
# define INTERNAL_NODE_KEY_SIZE sizeof(uint32_t)
# define INTERNAL_NODE_CHILD_SIZE sizeof(uint32_t)
# define INTERNAL_NODE_CELL_SIZE (INTERNAL_NODE_CHILD_SIZE + INTERNAL_NODE_KEY_SIZE)

# define INVALID_PAGE_NUM UINT32_MAX // change: added

// temporary
# define INTERNAL_NODE_MAX_CELLS 3 // keep small for testing
# define INTERNAL_NODE_MAX_KEYS 3 // keep small for testing

typedef struct {
    char* buffer;
    size_t buffer_length;
    ssize_t input_length;
} InputBuffer;

typedef enum {
    META_COMMAND_SUCCESS,
    META_COMMAND_UNRECOGNIZED_COMMAND
} MetaCommandResult;

typedef enum {
    PREPARE_SUCCESS,
    PREPARE_NEGATIVE_ID,
    PREPARE_STRING_TOO_LONG,
    PREPARE_SYNTAX_ERROR,
    PREPARE_UNRECOGNIZED_STATEMENT
} PrepareResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_DUPLICATE_KEY
} ExecuteResult;

typedef struct {
    uint32_t id;
    char username[COLUMN_USERNAME_SIZE + 1];
    char email[COLUMN_EMAIL_SIZE + 1];
} Row;

typedef struct {
    StatementType type;
    Row row_to_insert; // only used by insert statement
} Statement;

typedef struct {
    int file_descriptor;
    uint32_t file_length;
    uint32_t num_pages;
    void* pages[TABLE_MAX_PAGES];
} Pager;

typedef struct {
    Pager* pager;
    uint32_t root_page_num;
} Table;

typedef struct {
    Table* table;
    uint32_t page_num;
    uint32_t cell_num;
    bool end_of_table;
} Cursor;

typedef enum {
    NODE_INTERNAL,
    NODE_LEAF
} NodeType;

// function declarations
Table* db_open(const char* filename);
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table);
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement);
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement);
ExecuteResult execute_statement(Statement* statement, Table* table, InputBuffer* input_buffer);
ExecuteResult execute_insert(Statement* statement, Table* table);
ExecuteResult execute_select(Statement* statement, Table* table, InputBuffer* input_buffer);
void read_input(InputBuffer* input_buffer);
void print_row(Row* row, char* field);
void print_prompt();
ssize_t getline(char **lineptr, size_t *n, FILE *stream);
InputBuffer* new_input_buffer();
void close_input_buffer(InputBuffer* input_buffer);
void serialize_row(Row* source, void* destination);
void deserialize_row(void* source, Row* destination);
void* cursor_value(Cursor* cursor);
Pager* pager_open(const char* filename);
void* get_page(Pager* pager, uint32_t page_num);
void pager_flush(Pager* pager, uint32_t page_num);
void db_close(Table* table);
void free_table(Table* table);
Cursor* table_start(Table* table);
void cursor_advance(Cursor* cursor);
uint32_t* leaf_node_num_cells(void* node);
void* leaf_node_cell(void* node, uint32_t cell_num);
uint32_t* leaf_node_key(void* node, uint32_t cell_num);
void* leaf_node_value(void* node, uint32_t cell_num);
void initialize_leaf_node(void* node);
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value);
void print_constants();
Cursor* table_find(Table* table, uint32_t key);
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key);
NodeType get_node_type(void* node);
void set_node_type(void* node, NodeType type);
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value);
uint32_t get_unused_page_num(Pager* pager);
void create_new_root(Table* table, uint32_t right_child_page_num);
bool is_node_root(void* node);
void set_node_root(void* node, bool is_root);
void initialize_internal_node(void* node);
void indent(uint32_t level);
void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level);
uint32_t* internal_node_num_keys(void* node);
uint32_t* internal_node_right_child(void* node);
uint32_t* internal_node_cell(void* node, uint32_t cell_num);
uint32_t* internal_node_child(void* node, uint32_t child_num);
uint32_t* internal_node_key(void* node, uint32_t key_num);
uint32_t get_node_max_key(Pager* pager, void* node);
Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key);
uint32_t* leaf_node_next_leaf(void* node);
void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key);
uint32_t internal_node_find_child(void* node, uint32_t key);
void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num);
uint32_t* node_parent(void* node);
void internal_node_split_and_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num);


// function to open the database
Table* db_open(const char* filename) {
    Pager* pager = pager_open(filename);
    
    Table* table = malloc(sizeof(Table));
    table->pager = pager;
    table->root_page_num = 0;

    if (pager->num_pages == 0) {
        // New database file. Initialize page 0 as leaf node
        void* root_node = get_page(pager, 0);
        initialize_leaf_node(root_node);
        set_node_root(root_node, true);
    }

    return table;
}

// function to handle meta commands
MetaCommandResult do_meta_command(InputBuffer* input_buffer, Table* table) {
    if (strcmp(input_buffer->buffer, ".exit") == 0) {
        close_input_buffer(input_buffer);
        db_close(table);
        exit(EXIT_SUCCESS);
    } else if (strcmp(input_buffer->buffer, ".btree") == 0) {
        printf("Tree:\n");
        print_tree(table->pager, 0, 0);
        return META_COMMAND_SUCCESS;
    } else if (strcmp(input_buffer->buffer, ".constants") == 0) {
        printf("Constants:\n");
        print_constants();
        return META_COMMAND_SUCCESS;
        
    } else {
        return META_COMMAND_UNRECOGNIZED_COMMAND;
    }
}

// function to prepare the statement
PrepareResult prepare_statement(InputBuffer* input_buffer, Statement* statement) {
    if (strncmp(input_buffer->buffer, "insert", 6) == 0) {
        return prepare_insert(input_buffer, statement);
    }
    if (strncmp(input_buffer->buffer, "select", 6) == 0) {
        statement->type = STATEMENT_SELECT;
        return PREPARE_SUCCESS;
    }
    return PREPARE_UNRECOGNIZED_STATEMENT;
}

// function to execute the statement
ExecuteResult execute_statement(Statement* statement, Table* table, InputBuffer* input_buffer) {
    switch (statement->type) {
        case STATEMENT_INSERT:
            return execute_insert(statement, table);
        case STATEMENT_SELECT:
            return execute_select(statement, table, input_buffer);
    }
}

// function to print a row
void print_row(Row* row, char* field) {
    if (strcmp(field, "id") == 0) {
        printf("%d\n", row->id);
    } else if (strcmp(field, "username") == 0) {
        printf("%s\n", row->username);
    } else if (strcmp(field, "email") == 0) {
        printf("%s\n", row->email);
    } else {
        printf("(%d, %s, %s)\n", row->id, row->username, row->email);
    }
}

// function to create new input buffer
InputBuffer* new_input_buffer() {
    InputBuffer* input_buffer = malloc(sizeof(InputBuffer));
    input_buffer->buffer = NULL;
    input_buffer->buffer_length = 0;
    input_buffer->input_length = 0;

    return input_buffer;
}

// function to print prompt
void print_prompt() {
    printf("db > ");
}

// function to read line of input - it is included for linux, have to make it manual for windows
ssize_t getline(char **lineptr, size_t *n, FILE *stream) {
    size_t pos;
    int c;

    // check if any of the pointers are null
    if (lineptr == NULL || n == NULL || stream == NULL) {
        return -1;
    }

    // read first character from the stream
    c = fgetc(stream);
    if (c == EOF /*End Of File*/) {
        return -1;
    }

    // if *lineptr is NULL, allocate memory for it
    if (*lineptr == NULL) {
        *n = 128; // initial size of the buffer
        *lineptr = (char *)malloc(*n);
        if (*lineptr == NULL) {
            return -1;
        }
    }

    pos = 0; // position in the buffer
    while (c != EOF) {
        // if the buffer is full, reallocate with larger size
        if (pos + 1 >= *n) {
            // increase size by 25%
            size_t new_size = *n + (*n >> 2 /*shift 2 bits to the right = divide by 4*/);
            if (new_size < 128) {
                new_size = 128;
            }
            char *new_ptr = (char *)realloc(*lineptr, new_size);
            if (new_ptr == NULL) {
                return -1;
            }
            *n = new_size;
            *lineptr = new_ptr;
        }

        // store the character in the buffer
        ((unsigned char *)(*lineptr))[pos++] = c;
        if (c == '\n') {
            break;
        }
        c = fgetc(stream); // read next character
    }

    // null-terminate the buffer
    (*lineptr)[pos] = '\0';
    return pos; // return the number of characters read
}

// function to read input
void read_input(InputBuffer* input_buffer) {
    ssize_t bytes_read = getline(&(input_buffer->buffer), &(input_buffer->buffer_length), stdin);

    if (bytes_read <= 0) {
        printf("Error reading input\n");
        exit(EXIT_FAILURE);
    }

    // Ignore trailing newline
    input_buffer->input_length = bytes_read - 1;
    input_buffer->buffer[bytes_read - 1] = 0;
}

// function to free the memory of an input buffer
void close_input_buffer(InputBuffer* input_buffer) {
    free(input_buffer->buffer);
    free(input_buffer);
}

// function to serialize a row
void serialize_row (Row* source, void* destination) {
    memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
    memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
    memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

// function to deserialize a row
void deserialize_row(void *source, Row* destination) {
    memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
    memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
    memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

// function to free the memory of a table
void free_table(Table* table) {
    Pager* pager = table->pager;
    for (int i = 0; i < TABLE_MAX_PAGES && pager->pages[i] != NULL; i++) {
        free(pager->pages[i]);
    }
    free(pager);
    free(table);
}

// function to get a page from the pager
void* get_page(Pager* pager, uint32_t page_num) {
    if (page_num > TABLE_MAX_PAGES) {
        printf("Tried to fetch page number out of bounds. %d > %d\n", page_num, TABLE_MAX_PAGES);
        exit(EXIT_FAILURE);
    }

    if (pager->pages[page_num] == NULL) {
        // Cache miss. Allocate memory and load from file
        void* page = malloc(PAGE_SIZE);
        uint32_t num_pages = pager->file_length / PAGE_SIZE;

        // We might save a partial page at the end of the file
        if (pager->file_length % PAGE_SIZE) {
            num_pages += 1;
        }

        if (page_num <= num_pages) {
            lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
            ssize_t bytes_read = read(pager->file_descriptor, page, PAGE_SIZE);
            if (bytes_read == -1) {
                printf("Error reading file: %d\n", errno);
                exit(EXIT_FAILURE);
            }
        }

        pager->pages[page_num] = page;

        if (page_num >= pager->num_pages) {
            pager->num_pages = page_num + 1;
        }
    }
    return pager->pages[page_num];
}

// function to get a row slot from the table
void* cursor_value(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* page = get_page(cursor->table->pager, page_num);
    
    return leaf_node_value(page, cursor->cell_num);
}

// function to flush a page to the pager
void pager_flush(Pager* pager, uint32_t page_num) {
    if (pager->pages[page_num] == NULL) {
        printf("Tried to flush null page\n");
        exit(EXIT_FAILURE);
    }

    off_t offset = lseek(pager->file_descriptor, page_num * PAGE_SIZE, SEEK_SET);
    if (offset == -1) {
        printf("Error seeking: %d\n", errno);
        exit(EXIT_FAILURE);
    }

    ssize_t bytes_written = write(pager->file_descriptor, pager->pages[page_num], PAGE_SIZE);
    if (bytes_written == -1) {
        printf("Error writing: %d\n", errno);
        exit(EXIT_FAILURE);
    }
}

// function to close the database
void db_close (Table* table) {
    Pager* pager = table->pager;

    for (uint32_t i=0; i < pager->num_pages; i++) {
        if (pager->pages[i] == NULL) {
            continue;
        }
        pager_flush(pager, i);
        free(pager->pages[i]);
        pager->pages[i] = NULL;
    }

    int result = close(pager->file_descriptor);
    if (result == -1) {
        printf("Error closing db file.\n");
        exit(EXIT_FAILURE);
    }
    for (uint32_t i=0; i<TABLE_MAX_PAGES; i++) {
        void* page = pager->pages[i];
        if (page) {
            free(page);
            pager->pages[i] = NULL;
        }
    }
    free(pager);
    free(table);
}

// function to prepare the insert statement
PrepareResult prepare_insert(InputBuffer* input_buffer, Statement* statement) {
    statement->type = STATEMENT_INSERT;
    
    char* keyword = strtok(input_buffer->buffer, " ");
    char* id_string = strtok(NULL, " ");
    char* username = strtok(NULL, " ");
    char* email = strtok(NULL, " ");

    if (id_string == NULL || username == NULL || email == NULL) {
        return PREPARE_SYNTAX_ERROR;
    }

    int id = atoi(id_string);
    if (id < 0) {
        return PREPARE_NEGATIVE_ID;
    }
    if (strlen(username) > COLUMN_USERNAME_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }
    if (strlen(email) > COLUMN_EMAIL_SIZE) {
        return PREPARE_STRING_TOO_LONG;
    }

    statement->row_to_insert.id = id;
    strcpy(statement->row_to_insert.username, username);
    strcpy(statement->row_to_insert.email, email);

    return PREPARE_SUCCESS;
}

// function to execute the insert statement
ExecuteResult execute_insert(Statement* statement, Table* table) {
    Row* row_to_insert = &(statement->row_to_insert);
    uint32_t key_to_insert = row_to_insert->id;
    Cursor* cursor = table_find(table, key_to_insert);

    void* node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *(leaf_node_num_cells(node));

    if (cursor->cell_num < num_cells) {
        uint32_t key_at_index = *leaf_node_key(node, cursor->cell_num);
        if (key_at_index == key_to_insert) {
            return EXECUTE_DUPLICATE_KEY;
        }
    }

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

    free(cursor);

    return EXECUTE_SUCCESS;
}

// function to execute the select statement
ExecuteResult execute_select(Statement* statement, Table* table, InputBuffer* input_buffer) {
    Cursor* cursor = table_start(table);

    char* keyword = strtok(input_buffer->buffer, " ");
    char* field = strtok(NULL, " ");

    Row row;
    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row, field);
        cursor_advance(cursor);
    }
    free(cursor);
    
    return EXECUTE_SUCCESS;
}

// function to open a pager
Pager* pager_open(const char* filename) {
    int fd = open(filename,
                O_RDWR | // read/write mode
                    O_CREAT, // create file if it does not exist
                S_IWUSR | // user write permission
                    S_IRUSR); // user read permission
    if (fd == -1) {
        printf("Unable to open file\n");
        exit(EXIT_FAILURE);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    Pager* pager = malloc(sizeof(Pager));
    pager->file_descriptor = fd;
    pager->file_length = file_length;
    pager->num_pages = (file_length / PAGE_SIZE);

    if (file_length % PAGE_SIZE != 0) {
        printf("Db file is not a whole number of pages. Corrupt file.\n");
        exit(EXIT_FAILURE);
    }

    for (uint32_t i=0; i<TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }
    return pager;
}

// cursor pointing to the start of the table
Cursor* table_start(Table* table) {
    Cursor* cursor = table_find(table, 0);

    void* node = get_page(table->pager, cursor->page_num);
    uint32_t num_cells = *(leaf_node_num_cells(node));
    cursor->end_of_table = (num_cells == 0);

    return cursor;
}

// function to advance the cursor
void cursor_advance(Cursor* cursor) {
    uint32_t page_num = cursor->page_num;
    void* node = get_page(cursor->table->pager, page_num);

    cursor->cell_num += 1;
    if (cursor->cell_num >= *(leaf_node_num_cells(node))) {
        // Move to next leaf node
        uint32_t next_page_num = *leaf_node_next_leaf(node);
        if (next_page_num == 0) {
            // This was rightmost leaf
            cursor->end_of_table = true;
        } else {
            cursor->page_num = next_page_num;
            cursor->cell_num = 0;
        }
    }
}

// function to get the number of cells in a leaf node
uint32_t* leaf_node_num_cells(void* node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

// function to get a cell in a leaf node
void* leaf_node_cell(void* node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

// function to get the key of a cell in a leaf node
uint32_t* leaf_node_key(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}

// function to get the value of a cell in a leaf node
void* leaf_node_value(void* node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE;
}

// function to initialize a leaf node
void initialize_leaf_node(void* node) {
    set_node_type(node, NODE_LEAF);
    set_node_root(node, false);
    *leaf_node_num_cells(node) = 0;
    *leaf_node_next_leaf(node) = 0; // 0 represents no sibling
}

// function to insert key/value pair into a leaf node
void leaf_node_insert(Cursor* cursor, uint32_t key, Row* value) {
    void* node = get_page(cursor->table->pager, cursor->page_num);

    uint32_t num_cells = *(leaf_node_num_cells(node));
    if (num_cells >= LEAF_NODE_MAX_CELLS) {
        // Node full
        leaf_node_split_and_insert(cursor, key, value);
        return;
    }

    if (cursor->cell_num < num_cells) {
        // Make room for new cell
        for (uint32_t i=num_cells; i>cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i-1), LEAF_NODE_CELL_SIZE);
        }
    }

    *(leaf_node_num_cells(node)) += 1;
    *(leaf_node_key(node, cursor->cell_num)) = key;
    serialize_row(value, leaf_node_value(node, cursor->cell_num));
}

// function to print the constants
void print_constants() {
    printf("ROW_SIZE: %d\n", ROW_SIZE);
    printf("COMMON_NODE_HEADER_SIZE: %d\n", COMMON_NODE_HEADER_SIZE);
    printf("LEAF_NODE_HEADER_SIZE: %d\n", LEAF_NODE_HEADER_SIZE);
    printf("LEAF_NODE_CELL_SIZE: %d\n", LEAF_NODE_CELL_SIZE);
    printf("LEAF_NODE_SPACE_FOR_CELLS: %d\n", LEAF_NODE_SPACE_FOR_CELLS);
    printf("LEAF_NODE_MAX_CELLS: %d\n", LEAF_NODE_MAX_CELLS);
}

// function to find a key in the table, or the postion where it should be inserted
Cursor* table_find(Table* table, uint32_t key) {
    uint32_t root_page_num = table->root_page_num;
    void* root_node = get_page(table->pager, root_page_num);

    if (get_node_type(root_node) == NODE_LEAF) {
        return leaf_node_find(table, root_page_num, key);
    } else {
        return internal_node_find(table, root_page_num, key);
    }
}

// function to find a key in a leaf node
Cursor* leaf_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);
    uint32_t num_cells = *(leaf_node_num_cells(node));

    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = page_num;
    cursor->end_of_table = false;

    // Binary search
    uint32_t min_index = 0;
    uint32_t one_past_max_index = num_cells;
    while (one_past_max_index != min_index) {
        uint32_t index = (min_index + one_past_max_index) / 2;
        uint32_t key_at_index = *leaf_node_key(node, index);
        if (key == key_at_index) {
            cursor->cell_num = index;
            return cursor;
        }
        if (key < key_at_index) {
            one_past_max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    cursor->cell_num = min_index;
    return cursor;
}

// function to get the node type
NodeType get_node_type(void* node) {
    uint8_t value = *((uint8_t*)(node + NODE_TYPE_OFFSET));
    return (NodeType)value;
}

// function to set the node type
void set_node_type(void* node, NodeType type) {
    uint8_t value = type;
    *((uint8_t*)(node + NODE_TYPE_OFFSET)) = value;
}

// function to split a leaf node and insert a key/value pair
void leaf_node_split_and_insert(Cursor* cursor, uint32_t key, Row* value) {
    // Create a new node and move half the cells over
    // Insert the new value in one of the two nodes
    // Update parent or create a new parent
    void* old_node = get_page(cursor->table->pager, cursor->page_num);
    uint32_t old_max = get_node_max_key(cursor->table->pager, old_node);
    uint32_t new_page_num = get_unused_page_num(cursor->table->pager);
    void* new_node = get_page(cursor->table->pager, new_page_num);
    initialize_leaf_node(new_node);
    *node_parent(new_node) = *node_parent(old_node);
    *(leaf_node_next_leaf(new_node)) = *(leaf_node_next_leaf(old_node));
    *(leaf_node_next_leaf(old_node)) = new_page_num;

    // All existing keys plus new key should be divided evenly between old (left) and new (right) nodes
    // Starting from the right, move each key to correct position
    for (int32_t i=LEAF_NODE_MAX_CELLS; i>=0; i--) {
        void* destination_node;
        if (i >= LEAF_NODE_LEFT_SPLIT_COUNT) {
            destination_node = new_node;
        } else {
            destination_node = old_node;
        }
        uint32_t index_within_node = i % LEAF_NODE_LEFT_SPLIT_COUNT;
        void* destination = leaf_node_cell(destination_node, index_within_node);

        if (i == cursor->cell_num) {
            serialize_row(value, leaf_node_value(destination_node, index_within_node));
            *(leaf_node_key(destination_node, index_within_node)) = key;
        } else if (i > cursor->cell_num) {
            memcpy(destination, leaf_node_cell(old_node, i-1), LEAF_NODE_CELL_SIZE);
        } else {
            memcpy(destination, leaf_node_cell(old_node, i), LEAF_NODE_CELL_SIZE);
        }
    }

    // Update cell count on both leaf nodes
    *(leaf_node_num_cells(old_node)) = LEAF_NODE_LEFT_SPLIT_COUNT;
    *(leaf_node_num_cells(new_node)) = LEAF_NODE_RIGHT_SPLIT_COUNT;

    // Update parent
    if (is_node_root(old_node)) {
        return create_new_root(cursor->table, new_page_num);
    } else {
        uint32_t parent_page_num = *node_parent(old_node);
        uint32_t new_max = get_node_max_key(cursor->table->pager, old_node);
        void* parent = get_page(cursor->table->pager, parent_page_num);

        update_internal_node_key(parent, old_max, new_max);
        internal_node_insert(cursor->table, parent_page_num, new_page_num);
        return;
    }
}

// Until recycling is implemented, new pages will always be added to the end of the database file
uint32_t get_unused_page_num(Pager* pager) {
    return pager->num_pages;
}

// function to create a new root node
void create_new_root(Table* table, uint32_t right_child_page_num) {
    // Handle splitting the root
    // Old root copied to new page, becomes left child
    // Address of right child passed in
    // Re-initialize root page to contain the new root node
    // New root node points to 2 children
    void* root = get_page(table->pager, table->root_page_num);
    void* right_child = get_page(table->pager, right_child_page_num);
    uint32_t left_child_page_num = get_unused_page_num(table->pager);
    void* left_child = get_page(table->pager, left_child_page_num);

    if (get_node_type(root) == NODE_INTERNAL) {
        initialize_internal_node(right_child);
        initialize_internal_node(left_child);
    }

    // Left child has data copied from old root
    memcpy(left_child, root, PAGE_SIZE);
    set_node_root(left_child, false);

    if (get_node_type(left_child) == NODE_INTERNAL) {
        void* child;
        for (int i = 0; i < *internal_node_num_keys(left_child); i++) {
            child = get_page(table->pager, *internal_node_child(left_child,i));
            *node_parent(child) = left_child_page_num;
        }
        child = get_page(table->pager, *internal_node_right_child(left_child));
        *node_parent(child) = left_child_page_num;
    }

    // Root node is a new internal node with one key and two children
    initialize_internal_node(root);
    set_node_root(root, true);
    *internal_node_num_keys(root) = 1;
    *internal_node_child(root, 0) = left_child_page_num;
    uint32_t left_child_max_key = get_node_max_key(table->pager, left_child);
    *internal_node_key(root, 0) = left_child_max_key;
    *internal_node_right_child(root) = right_child_page_num;
    *node_parent(left_child) = table->root_page_num;
    *node_parent(right_child) = table->root_page_num;
}

// function to get the number of keys in an internal node
uint32_t* internal_node_num_keys(void* node) {
    return node + INTERNAL_NODE_NUM_KEYS_OFFSET;
}

// function to get the right child of an internal node
uint32_t* internal_node_right_child(void* node) {
    return node + INTERNAL_NODE_RIGHT_CHILD_OFFSET;
}

// function to get a cell from an internal node
uint32_t* internal_node_cell(void* node, uint32_t cell_num) {
    return node + INTERNAL_NODE_HEADER_SIZE + cell_num * INTERNAL_NODE_CELL_SIZE;
}

// function to get a child from an internal node ((changed))
uint32_t* internal_node_child(void* node, uint32_t child_num) {
    uint32_t num_keys = *internal_node_num_keys(node);
    if (child_num > num_keys) {
      printf("Tried to access child_num %d > num_keys %d\n", child_num, num_keys);
      exit(EXIT_FAILURE);
    } else if (child_num == num_keys) {
      uint32_t* right_child = internal_node_right_child(node);
      if (*right_child == INVALID_PAGE_NUM) {
        printf("Tried to access right child of node, but was invalid page\n");
        exit(EXIT_FAILURE);
      }
      return right_child;
    } else {
      uint32_t* child = internal_node_cell(node, child_num);
      if (*child == INVALID_PAGE_NUM) {
        printf("Tried to access child %d of node, but was invalid page\n", child_num);
        exit(EXIT_FAILURE);
      }
      return child;
    }
  }

// function to get a key from an internal node
uint32_t* internal_node_key(void* node, uint32_t key_num) {
    return (void*)internal_node_cell(node, key_num) + INTERNAL_NODE_CHILD_SIZE;
}

// function to get the maximum key of a node ((change: added Pager parameter))
uint32_t get_node_max_key(Pager* pager, void* node) {
    if (get_node_type(node) == NODE_LEAF) {
      return *leaf_node_key(node, *leaf_node_num_cells(node) - 1);
    }
    void* right_child = get_page(pager,*internal_node_right_child(node));
    return get_node_max_key(pager, right_child);
  }

// function to check if the node is root
bool is_node_root(void* node) {
    uint8_t value = *((uint8_t*)(node + IS_ROOT_OFFSET));
    return (bool)value;
}

// function to set the node as root
void set_node_root(void* node, bool is_root) {
    uint8_t value = is_root;
    *((uint8_t*)(node + IS_ROOT_OFFSET)) = value;
}

// function to initialize an internal node ((change: added comment and below))
void initialize_internal_node(void* node) {
    set_node_type(node, NODE_INTERNAL);
    set_node_root(node, false);
    *(internal_node_num_keys(node)) = 0;
    /*
    Necessary because the root page number is 0; by not initializing an internal 
    node's right child to an invalid page number when initializing the node, we may
    end up with 0 as the node's right child, which makes the node a parent of the root
    */
    *internal_node_right_child(node) = INVALID_PAGE_NUM;
}

// function to add the indentetion when printing the tree
void indent(uint32_t level) {
    for (uint32_t i=0; i<level; i++) {
        printf("  ");
    }
}

// function to print the tree ((changed))
void print_tree(Pager* pager, uint32_t page_num, uint32_t indentation_level) {
    void* node = get_page(pager, page_num);
    uint32_t num_keys, child;
  
    switch (get_node_type(node)) {
      case (NODE_LEAF):
        num_keys = *leaf_node_num_cells(node);
        indent(indentation_level);
        printf("- leaf (size %d)\n", num_keys);
        for (uint32_t i = 0; i < num_keys; i++) {
          indent(indentation_level + 1);
          printf("- %d\n", *leaf_node_key(node, i));
        }
        break;
      case (NODE_INTERNAL):
        num_keys = *internal_node_num_keys(node);
        indent(indentation_level);
        printf("- internal (size %d)\n", num_keys);
        if (num_keys > 0) {
          for (uint32_t i = 0; i < num_keys; i++) {
            child = *internal_node_child(node, i);
            print_tree(pager, child, indentation_level + 1);
  
            indent(indentation_level + 1);
            printf("- key %d\n", *internal_node_key(node, i));
          }
          child = *internal_node_right_child(node);
          print_tree(pager, child, indentation_level + 1);
        }
        break;
    }
}

// function to find child node containing the given key
uint32_t internal_node_find_child(void* node, uint32_t key) {
    // Return the index of the child which should contain the given key
    uint32_t num_keys = *internal_node_num_keys(node);

    // Binary search
    uint32_t min_index = 0;
    uint32_t max_index = num_keys; // there is one more child than key
    while (min_index != max_index) {
        uint32_t index = (min_index + max_index) / 2;
        uint32_t key_to_right = *internal_node_key(node, index);
        if (key_to_right >= key) {
            max_index = index;
        } else {
            min_index = index + 1;
        }
    }

    return min_index;
}

// function to find a key in an internal node
Cursor* internal_node_find(Table* table, uint32_t page_num, uint32_t key) {
    void* node = get_page(table->pager, page_num);

    uint32_t child_index = internal_node_find_child(node, key);
    uint32_t child_num = *internal_node_child(node, child_index);
    void* child = get_page(table->pager, child_num);
    switch (get_node_type(child)) {
        case NODE_LEAF:
            return leaf_node_find(table, child_num, key);
        case NODE_INTERNAL:
            return internal_node_find(table, child_num, key);
    }
}

// function to access next leaf node
uint32_t* leaf_node_next_leaf(void* node) {
    return node + LEAF_NODE_NEXT_LEAF_OFFSET;
}

// function to get the parent of a node
uint32_t* node_parent(void* node) {
    return node + PARENT_POINTER_OFFSET;
}

void update_internal_node_key(void* node, uint32_t old_key, uint32_t new_key) {
    uint32_t old_child_index = internal_node_find_child(node, old_key);
    *internal_node_key(node, old_child_index) = new_key;
}

// function to insert a new child to an internal node
void internal_node_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
    // Add a new child/key pair to parent that corresponds to child
    void* parent = get_page(table->pager, parent_page_num);
    void* child = get_page(table->pager, child_page_num);
    uint32_t child_max_key = get_node_max_key(table->pager, child);
    uint32_t index = internal_node_find_child(parent, child_max_key);
  
    uint32_t original_num_keys = *internal_node_num_keys(parent);
  
    if (original_num_keys >= INTERNAL_NODE_MAX_KEYS) {
        internal_node_split_and_insert(table, parent_page_num, child_page_num);
        return;
    }
  
    uint32_t right_child_page_num = *internal_node_right_child(parent);
    // An internal node with a right child of INVALID_PAGE_NUM is empty
    if (right_child_page_num == INVALID_PAGE_NUM) {
        *internal_node_right_child(parent) = child_page_num;
        return;
    }
  
    void* right_child = get_page(table->pager, right_child_page_num);
    // If we are already at the max number of cells for a node, we cannot increment before splitting.
    // Incrementing without inserting a new key/child pair and immediately calling
    //  internal_node_split_and_insert has the effect of creating a new key at
    //  (max_cells + 1) with an uninitialized value
    *internal_node_num_keys(parent) = original_num_keys + 1;
  
    if (child_max_key > get_node_max_key(table->pager, right_child)) {
        // Replace right child
        *internal_node_child(parent, original_num_keys) = right_child_page_num;
        *internal_node_key(parent, original_num_keys) = get_node_max_key(table->pager, right_child);
        *internal_node_right_child(parent) = child_page_num;
    } else {
        /* Make room for the new cell */
        for (uint32_t i = original_num_keys; i > index; i--) {
            void* destination = internal_node_cell(parent, i);
            void* source = internal_node_cell(parent, i - 1);
            memcpy(destination, source, INTERNAL_NODE_CELL_SIZE);
        }
        *internal_node_child(parent, index) = child_page_num;
        *internal_node_key(parent, index) = child_max_key;
    }
  }

// function to split an internal node and insert a new child
void internal_node_split_and_insert(Table* table, uint32_t parent_page_num, uint32_t child_page_num) {
    uint32_t old_page_num = parent_page_num;
    void* old_node = get_page(table->pager,parent_page_num);
    uint32_t old_max = get_node_max_key(table->pager, old_node);
  
    void* child = get_page(table->pager, child_page_num); 
    uint32_t child_max = get_node_max_key(table->pager, child);
  
    uint32_t new_page_num = get_unused_page_num(table->pager);
  
    // Declaring a flag before updating pointers which records whether this operation involves splitting the root
    // - if it does, we will insert our newly created node during the step where the table's new root is created.
    // - If it does not, we have to insert the newly created node into its parent after the old node's keys have been transferred over.
    // We are not able to do this if the newly created node's parent is not a newly initialized root node,
    //  because in that case its parent may have existing keys aside from our old node which we are splitting.
    // If that is true, we need to find a place for our newly created node in its parent,
    //  and we cannot insert it at the correct index if it does not yet have any keys

    uint32_t splitting_root = is_node_root(old_node);
  
    void* parent;
    void* new_node;
    if (splitting_root) {
        create_new_root(table, new_page_num);
        parent = get_page(table->pager,table->root_page_num);
        
        // If we are splitting the root, we need to update old_node to point to the new root's left child,
        // new_page_num will already point to the new root's right child
        old_page_num = *internal_node_child(parent,0);
        old_node = get_page(table->pager, old_page_num);
    } else {
        parent = get_page(table->pager,*node_parent(old_node));
        new_node = get_page(table->pager, new_page_num);
        initialize_internal_node(new_node);
    }
    
    uint32_t* old_num_keys = internal_node_num_keys(old_node);
  
    uint32_t cur_page_num = *internal_node_right_child(old_node);
    void* cur = get_page(table->pager, cur_page_num);
  
    // First put right child into new node and set right child of old node to invalid page number
    internal_node_insert(table, new_page_num, cur_page_num);
    *node_parent(cur) = new_page_num;
    *internal_node_right_child(old_node) = INVALID_PAGE_NUM;

    // For each key until you get to the middle key, move the key and the child to the new node
    for (int i = INTERNAL_NODE_MAX_KEYS - 1; i > INTERNAL_NODE_MAX_KEYS / 2; i--) {
        cur_page_num = *internal_node_child(old_node, i);
        cur = get_page(table->pager, cur_page_num);
  
        internal_node_insert(table, new_page_num, cur_page_num);
        *node_parent(cur) = new_page_num;
  
        (*old_num_keys)--;
    }
  
    // Set child before middle key, which is now the highest key, to be node's right child, and decrement number of keys
    *internal_node_right_child(old_node) = *internal_node_child(old_node,*old_num_keys - 1);
    (*old_num_keys)--;
  
    // Determine which of the two nodes after the split should contain the child to be inserted, and insert the child
    uint32_t max_after_split = get_node_max_key(table->pager, old_node);
  
    uint32_t destination_page_num = child_max < max_after_split ? old_page_num : new_page_num;
  
    internal_node_insert(table, destination_page_num, child_page_num);
    *node_parent(child) = destination_page_num;
  
    update_internal_node_key(parent, old_max, get_node_max_key(table->pager, old_node));
  
    if (!splitting_root) {
        internal_node_insert(table,*node_parent(old_node),new_page_num);
        *node_parent(new_node) = *node_parent(old_node);
    }
  }

// main function
int main(int argc, char* argv[]) {
    if (argc < 2) {
        printf("Must supply a database filename.\n");
        exit(EXIT_FAILURE);
    }

    char* filename = argv[1];
    Table* table = db_open(filename);

    InputBuffer* input_buffer = new_input_buffer();
    while (true) {
        print_prompt();
        read_input(input_buffer);

        // all meta commands start with '.' - here I handle them
        if (input_buffer->buffer[0] == '.') {
            switch (do_meta_command(input_buffer, table)) {
                case (META_COMMAND_SUCCESS):
                    continue;
                case (META_COMMAND_UNRECOGNIZED_COMMAND):
                    printf("Unrecognized command '%s'\n", input_buffer->buffer);
                    continue;
            }
        }

        Statement statement;
        switch (prepare_statement(input_buffer, &statement)) {
            case (PREPARE_SUCCESS):
                break;
            case (PREPARE_NEGATIVE_ID):
                printf("ID must be positive.\n");
                continue;
            case (PREPARE_STRING_TOO_LONG):
                printf("String is too long.\n");
                continue;
            case (PREPARE_SYNTAX_ERROR):
                printf("Syntax error. Could not parse statement.\n");
                continue;
            case (PREPARE_UNRECOGNIZED_STATEMENT):
                printf("Unrecognized keyword at start of '%s'\n", input_buffer->buffer);
                continue;
        }

        switch (execute_statement(&statement, table, input_buffer)) {
            case (EXECUTE_SUCCESS):
                printf("Executed.\n");
                break;
            case (EXECUTE_DUPLICATE_KEY):
                printf("Error: Duplicate key.\n");
                break;
        }
    }
}