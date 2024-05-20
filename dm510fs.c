#include <fuse.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>
#include <utime.h>
#include <sys/types.h>
#include <ctype.h> // For character manipulation functions - Caesar Cipher Shift


// Filesystem operations declarations
int dm510fs_getattr(const char *, struct stat *);
int dm510fs_readdir(const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info *);
int dm510fs_open(const char *, struct fuse_file_info *);
int dm510fs_read(const char *, char *, size_t, off_t, struct fuse_file_info *);
int dm510fs_write(const char *, const char *, size_t, off_t, struct fuse_file_info *);
int dm510fs_release(const char *, struct fuse_file_info *);
int dm510fs_mkdir(const char *, mode_t);
int dm510fs_mknod(const char *, mode_t, dev_t);
int dm510fs_unlink(const char *);
int dm510fs_rmdir(const char *);
int dm510fs_utime(const char *, struct utimbuf *);
int dm510fs_rename(const char *, const char *);
int dm510fs_truncate(const char *, off_t);
void *dm510fs_init();
void dm510fs_destroy(void *);

/*
 * See descriptions in fuse source code usually located in /usr/include/fuse/fuse.h
 * Notice: The version on Github is a newer version than installed at IMADA
 */
static struct fuse_operations dm510fs_oper = {
	.getattr	= dm510fs_getattr,
	.readdir	= dm510fs_readdir,
	.mknod = dm510fs_mknod,
	.mkdir = dm510fs_mkdir,
	.unlink = dm510fs_unlink,
	.rmdir = dm510fs_rmdir,
	.truncate = dm510fs_truncate,
	.open	= dm510fs_open,
	.read	= dm510fs_read,
	.release = dm510fs_release,
	.write = dm510fs_write,
	.rename = dm510fs_rename,
	.utime = dm510fs_utime,
	.init = dm510fs_init,
	.destroy = dm510fs_destroy
};

// Define constrants for limts within filesystem 
#define MAX_DATA_IN_FILE 256
#define MAX_PATH_LENGTH  256
#define MAX_NAME_LENGTH  256
#define MAX_INODES  4
#define FS_STATE_FILE "/home/dm510/dm510/linux-6.6.9/kernel/dm510/assignment3/fs_state.dat"
#define SHIFT_VAL 5 // Shift value for Caesar Cipher
#define DIRECT_DATA_SIZE 128  // Size of direct data area in bytes
#define NUM_POINTERS 10       // Number of pointers to data blocks
#define BLOCK_SIZE 4096       // Assuming a block size of 4KB
#define MAX_BLOCKS 100        // Number of blocks
#define max(a, b) ((a) > (b) ? (a) : (b))
#define min(a, b) ((a) < (b) ? (a) : (b))

// Global array to simulate disk blocks
char data_blocks[MAX_BLOCKS][BLOCK_SIZE];

/* The Inode for the filesystem*/
typedef struct Inode {
	bool is_active;     // Indicates whether the indoe is in use
	bool is_dir;        // Indicates whether the indoe is a directory
        char direct_data[DIRECT_DATA_SIZE]; // Directly stored data within the inode
        int data_block_pointers[NUM_POINTERS]; // Pointers to data blocks
        char name[MAX_NAME_LENGTH]; // Name of the file/directory
	size_t size;        // Total size of the file
	mode_t mode;        // Specify the file type and permissions
	nlink_t nlink;      // The link count
	time_t atime;       // Access time
	time_t mtime;       // Modificaton time
	time_t ctime;       //Change time
	char path[MAX_PATH_LENGTH]; // Path of the file/directory
} Inode;

// Array that represent the entire filesystem 
Inode filesystem[MAX_INODES];

// Caesar Cipher Encryption function
void caesar_encrypt(char *data, size_t size) {
    for (int i = 0; i < size; i++) {
        if (isalpha(data[i])) {
            char base = (isupper(data[i]) ? 'A' : 'a');
            data[i] = ((data[i] - base + SHIFT_VAL) % 26) + base;
        }
    }
}

// Caesar Cipher Decryption function
void caesar_decrypt(char *data, size_t size) {
    for (int i = 0; i < size; i++) {
        if (isalpha(data[i])) {
            char base = (isupper(data[i]) ? 'A' : 'a');
            data[i] = ((data[i] - base - SHIFT_VAL + 26) % 26) + base;
        }
    }
}

// Function to find an inode by path
int find_inode(const char *path) {
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0) {
            return i;
        }
    }
    return -1; // inode not found
}

// Function to clear the data in an inode
void clear_inode_data(Inode *inode) {
    memset(inode->direct_data, 0, DIRECT_DATA_SIZE);
    for (int i = 0; i < NUM_POINTERS; i++) {
        inode->data_block_pointers[i] = -1;  // Assuming -1 denotes unused block
    }
}

// Function to extract the parent directory from a given path
void extract_parent_directory(const char *path, char *parent_dir) {
    strcpy(parent_dir, path);
    char *last_slash = strrchr(parent_dir, '/');
    if (last_slash != NULL) {
        *last_slash = '\0';  // Null-terminate at the last slash
    }
}

// Function to remove a directory entry
void remove_directory_entry(const char *dir_path, const char *entry) {
    int dir_idx = find_inode(dir_path);
    if (dir_idx != -1) {
        // Implementation would depend on how directory entries are stored
        // This could involve marking an entry as inactive in a list
    }
}

// Function to check if a directory is empty
bool is_directory_empty(const Inode *dir) {
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strstr(filesystem[i].path, dir->path) == filesystem[i].path &&
            strcmp(filesystem[i].path, dir->path) != 0) {
            return false;
        }
    }
    return true;
}


void save_fs_state() {
    FILE *fp = fopen(FS_STATE_FILE, "wb");
    if (fp == NULL) {
        fprintf(stderr, "Failedf to open file for writing filesystem state\n");
        return;
    }
    fwrite(filesystem, sizeof(Inode), MAX_INODES, fp);
    fclose(fp);
}

void load_fs_state() {
    FILE *fp = fopen(FS_STATE_FILE, "rb");
    if (fp == NULL) {
        fprintf(stderr, "Failed to open file for reading filesystem state\n");
        return;
    }
    fread(filesystem, sizeof(Inode), MAX_INODES, fp);
    fclose(fp);
}

// Prints the path of the inode
void debug_inode(int i) {
	Inode inode = filesystem[i];

	printf("=============================================\n");
	printf("      Path: %s\n", inode.path);
	printf("=============================================\n");
}


/*
 * Return file attributes.
 * The "stat" structure is described in detail in the stat(2) manual page.
 * For the given pathname, this should fill in the elements of the "stat" structure.
 * If a field is meaningless or semi-meaningless (e.g., st_ino) then it should be set to 0 or given a "reasonable" value.
 * This call is pretty much required for a usable filesystem.
*/
int dm510fs_getattr( const char *path, struct stat *stbuf ) {
	printf("getattr: (path=%s)\n", path);

	memset(stbuf, 0, sizeof(struct stat));
	for( int i = 0; i < MAX_INODES; i++) {
		printf("===> %s  %s \n", path, filesystem[i].path);
		if( filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0 ) {
			printf("Found inode for path %s at location %i\n", path, i);
			stbuf->st_mode = filesystem[i].mode;
			stbuf->st_nlink = filesystem[i].nlink;
			stbuf->st_size = filesystem[i].size;
            stbuf->st_atime = filesystem[i].atime; // Access time
            stbuf->st_mtime = filesystem[i].mtime; // Modification time
			stbuf->st_ctime = filesystem[i].ctime; // Set the change time
			return 0;
		}
	}

	return -ENOENT;
}

/*
 * Return one or more directory entries (struct dirent) to the caller. This is one of the most complex FUSE functions.
 * Required for essentially any filesystem, since it's what makes ls and a whole bunch of other things work.
 * The readdir function is somewhat like read, in that it starts at a given offset and returns results in a caller-supplied buffer.
 * However, the offset not a byte offset, and the results are a series of struct dirents rather than being uninterpreted bytes.
 * To make life easier, FUSE provides a "filler" function that will help you put things into the buffer.
 *
 * The general plan for a complete and correct readdir is:
 *
 * 1. Find the first directory entry following the given offset (see below).
 * 2. Optionally, create a struct stat that describes the file as for getattr (but FUSE only looks at st_ino and the file-type bits of st_mode).
 * 3. Call the filler function with arguments of buf, the null-terminated filename, the address of your struct stat
 *    (or NULL if you have none), and the offset of the next directory entry.
 * 4. If filler returns nonzero, or if there are no more files, return 0.
 * 5. Find the next file in the directory.
 * 6. Go back to step 2.
 * From FUSE's point of view, the offset is an uninterpreted off_t (i.e., an unsigned integer).
 * You provide an offset when you call filler, and it's possible that such an offset might come back to you as an argument later.
 * Typically, it's simply the byte offset (within your directory layout) of the directory entry, but it's really up to you.
 *
 * It's also important to note that readdir can return errors in a number of instances;
 * in particular it can return -EBADF if the file handle is invalid, or -ENOENT if you use the path argument and the path doesn't exist.
*/
int dm510fs_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi) {
    printf("Readdir called on: %s\n", path);
    
    int dir_idx = find_inode(path);
    if (dir_idx == -1) {
        printf("Directory not found: %s\n", path);
        return -ENOENT;
    }

    if (!filesystem[dir_idx].is_dir) {
        printf("Not a directory: %s\n", path);
        return -ENOTDIR;
    }

    // Add current and parent directory entries
    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    // Iterate over all inodes and list active files and directories that are children of the directory
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active) {
            const char* full_path = filesystem[i].path;
            if (strstr(full_path, path) == full_path) { // Check if path is a prefix
                const char *subpath = full_path + strlen(path);
                // Ensure no leading slashes unless root
                if (subpath[0] == '/') subpath++;
                if (subpath[0] != '\0' && strchr(subpath, '/') == NULL) {
                    printf("Adding entry: %s\n", subpath);
                    if (filler(buf, subpath, NULL, 0) != 0) {
                        printf("Buffer full, cannot add more entries after %s\n", subpath);
                        return -ENOMEM;
                    }
                }
            }
        }
    }

    return 0;
}




/*
 * Open a file.
 * If you aren't using file handles, this function should just check for existence and permissions and return either success or an error code.
 * If you use file handles, you should also allocate any necessary structures and set fi->fh.
 * In addition, fi has some other fields that an advanced filesystem might find useful; see the structure definition in fuse_common.h for very brief commentary.
 * Link: https://github.com/libfuse/libfuse/blob/0c12204145d43ad4683136379a130385ef16d166/include/fuse_common.h#L50
*/
int dm510fs_open( const char *path, struct fuse_file_info *fi ) {
    printf("open: (path=%s)\n", path);
	return 0;
}

/*
 * Read size bytes from the given file into the buffer buf, beginning offset bytes into the file. See read(2) for full details.
 * Returns the number of bytes transferred, or 0 if offset was at or beyond the end of the file. Required for any sensible filesystem.
*/
int dm510fs_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    printf("read: (path=%s, offset=%ld, size=%ld)\n", path, offset, size);

    for (int i = 0; i < MAX_INODES; i++) {
        if (strcmp(filesystem[i].path, path) == 0 && filesystem[i].is_active) {
            if (offset >= filesystem[i].size) {
                return 0; // Offset is beyond the end of the file, nothing to read
            }

            size_t bytes_to_read = min(size, filesystem[i].size - offset);
            size_t bytes_read = 0;

            // Read from direct data if within range
            if (offset < DIRECT_DATA_SIZE) {
                size_t direct_read_size = min(DIRECT_DATA_SIZE - offset, bytes_to_read);
                memcpy(buf, filesystem[i].direct_data + offset, direct_read_size);
                buf += direct_read_size;
                bytes_read += direct_read_size;
                bytes_to_read -= direct_read_size;
                offset += direct_read_size;
            }

            // Continue reading from block data if needed
            while (bytes_to_read > 0 && offset < filesystem[i].size) {
                int block_index = (offset - DIRECT_DATA_SIZE) / BLOCK_SIZE;
                if (block_index >= NUM_POINTERS || filesystem[i].data_block_pointers[block_index] == -1) {
                    break; // No more data available or block not assigned
                }
                size_t block_offset = (offset - DIRECT_DATA_SIZE) % BLOCK_SIZE;
                size_t block_read_size = min(BLOCK_SIZE - block_offset, bytes_to_read);
                memcpy(buf, data_blocks[filesystem[i].data_block_pointers[block_index]] + block_offset, block_read_size);

                buf += block_read_size;
                bytes_read += block_read_size;
                bytes_to_read -= block_read_size;
                offset += block_read_size;
            }

            // Decrypt data after reading it from the filesystem
            caesar_decrypt(buf - bytes_read, bytes_read);

            printf("Decrypted message: %.*s\n", (int)bytes_read, buf - bytes_read);

            return bytes_read; // Return the actual number of bytes read
        }
    }
    return -ENOENT; // File not found
}




// Writes data to a file
int dm510fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    if (size == 0) return 0;  // No data to write

    // Allocate buffer for encrypted data
    char *encrypted_buf = malloc(size);
    if (!encrypted_buf) return -ENOMEM;  // Allocation failed
    memcpy(encrypted_buf, buf, size);

    // Encrypt the data before writing
    caesar_encrypt(encrypted_buf, size);

    // Log the encrypted message
    printf("Encrypted message being written: ");
    for (size_t i = 0; i < size; i++) {
        printf("%c", isprint(encrypted_buf[i]) ? encrypted_buf[i] : '.'); // Print dots for non-printable characters
    }
    printf("\n");

    int bytes_written = 0;
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0) {
            // Ensure not to write beyond the file system's data capacity
            if (offset + size > MAX_DATA_IN_FILE) {
                size = MAX_DATA_IN_FILE - offset;
            }

            // Write data within the direct data range
            if (offset < DIRECT_DATA_SIZE) {
                size_t to_write = min(size, DIRECT_DATA_SIZE - offset);
                memcpy(filesystem[i].direct_data + offset, encrypted_buf, to_write);
                bytes_written += to_write;
            }

            // Update offset for any additional data beyond the direct data area
            size_t remaining_size = size - bytes_written;
            offset += bytes_written;

            // Write to additional data blocks if needed and if remaining_size > 0
            while (remaining_size > 0 && offset < MAX_DATA_IN_FILE) {
                // Handle block-level writing if necessary
                // This part is left as an exercise to implement block allocation and writing
            }

            filesystem[i].size = max(filesystem[i].size, offset + remaining_size);
            free(encrypted_buf);
            return bytes_written; // Return the number of bytes written
        }
    }

    free(encrypted_buf);
    return -ENOENT; // File not found
}




int dm510fs_dump_raw(const char *path) {
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0) {
            printf("Direct data in '%s': %.*s\n", path, DIRECT_DATA_SIZE, filesystem[i].direct_data);
            
            // Optionally print indirect block data
            for (int j = 0; j < NUM_POINTERS; j++) {
                int block_idx = filesystem[i].data_block_pointers[j];
                if (block_idx >= 0) {
                    printf("Data block %d: %.*s\n", j, BLOCK_SIZE, data_blocks[block_idx]);
                }
            }
            return 0;
        }
    }
    return -ENOENT; // File not found
}


/* Make directories - TODO */
int dm510fs_mkdir(const char *path, mode_t mode) {
    if (find_inode(path) != -1) {
        printf("mkdir: Directory already exists at %s\n", path);
        return -EEXIST;  // Directory already exists
    }

    // Locate the first unused Inode in the filesystem
    for (int i = 0; i < MAX_INODES; i++) {
        if (!filesystem[i].is_active) {
            printf("mkdir: Found unused inode at location %i for %s\n", i, path);
            filesystem[i].is_active = true;
            filesystem[i].is_dir = true;
            filesystem[i].mode = S_IFDIR | mode;  // Apply requested permissions
            filesystem[i].nlink = 2;  // '.' and '..'
            strcpy(filesystem[i].path, path);

            debug_inode(i);
            return 0;  // Success
        }
    }

    return -ENOSPC;  // No space left for new inode
}


// Make files 
int dm510fs_mknod(const char *path, mode_t mode, dev_t rdev) {
    if (strlen(path) >= MAX_PATH_LENGTH) return -ENAMETOOLONG;

    if (find_inode(path) != -1) return -EEXIST;

    int inode_index = -1;
    for (int i = 0; i < MAX_INODES; i++) {
        if (!filesystem[i].is_active) {
            inode_index = i;
            break;
        }
    }

    if (inode_index == -1) {
        printf("No inode available\n");
        return -ENOSPC;  // No space left on device (no available inode)
    }

    // Initialize the new inode
    Inode *inode = &filesystem[inode_index];
    inode->is_active = true;
    inode->is_dir = (mode & S_IFDIR) != 0;
    inode->mode = mode | (inode->is_dir ? S_IFDIR : S_IFREG);
    inode->nlink = 1;
    inode->size = 0;
    strcpy(inode->path, path);

    printf("Created %s at inode %d\n", path, inode_index);

    return 0;
}



// Remove a file.
int dm510fs_unlink(const char *path) {
    // Find inode of the file
    int inode_index = find_inode(path);
    if (inode_index == -1) return -ENOENT;

    // Remove the file's data
    clear_inode_data(&filesystem[inode_index]); // Pass address of the inode

    // Set inode as inactive
    filesystem[inode_index].is_active = false;

    // Update parent directory to remove the file entry
    char parent_dir[MAX_PATH_LENGTH];
    extract_parent_directory(path, parent_dir);
    remove_directory_entry(parent_dir, path);

    return 0;
}



// Remove a directory.
int dm510fs_rmdir(const char *path) {
    int dir_index = find_inode(path);
    if (dir_index == -1) return -ENOENT;

    // Check if directory is empty
    if (!is_directory_empty(&filesystem[dir_index])) { // Pass address of the inode
        return -ENOTEMPTY;
    }

    // Set directory inode as inactive
    filesystem[dir_index].is_active = false;

    // Update parent directory
    char parent_dir[MAX_PATH_LENGTH];
    extract_parent_directory(path, parent_dir);
    remove_directory_entry(parent_dir, path);

    return 0;
}



// Renames a file or directory.
int dm510fs_rename(const char *oldpath, const char *newpath) {
    printf("Attempting to rename from %s to %s\n", oldpath, newpath);

    int src_idx = -1, dest_idx = -1;
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, oldpath) == 0) {
            src_idx = i;
        }
        if (filesystem[i].is_active && strcmp(filesystem[i].path, newpath) == 0) {
            dest_idx = i;
        }
    }

    if (src_idx == -1) {
        printf("Source file not found\n");
        return -ENOENT;
    }

    if (dest_idx != -1) {
        // Handle non-empty directory or existing file cases
        printf("Destination path already exists\n");
        return -EEXIST;
    }

    // Update the inode for new path and ensure directory consistency
    strncpy(filesystem[src_idx].path, newpath, MAX_PATH_LENGTH);
    printf("Successfully renamed %s to %s\n", oldpath, newpath);

    return 0;
}



// Updates the access and modification times of a file with nanosecond precision.
int dm510fs_utime(const char *path, struct utimbuf *ubuf) {
    fprintf(stderr, "utime: (path=%s)\n", path);

    // Find the inode corresponding to the path
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0) {
            // Update the inode's timestamps
            time_t now = time(NULL); // Get current time
            if (ubuf != NULL) {
                // If times are provided, update to those times
                filesystem[i].atime = ubuf->actime;
                filesystem[i].mtime = ubuf->modtime;
            } else {
                // If no times are provided, set to current time
                filesystem[i].atime = now;
                filesystem[i].mtime = now;
            }
	    // Update the ctime to the current time
	    filesystem[i].ctime = now; 
            return 0; // Success
        }
    }

    fprintf(stderr, "utime: File not found %s\n", path);
    return -ENOENT; // No such file
}

// Changes size of a file 
int dm510fs_truncate(const char *path, off_t new_size) {
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0) {
            if (new_size < filesystem[i].size) {
                // Clear data beyond new size within direct data
                if (new_size < DIRECT_DATA_SIZE) {
                    memset(filesystem[i].direct_data + new_size, 0, DIRECT_DATA_SIZE - new_size);
                }
                // Handle clearing block data if needed
            } else if (new_size > filesystem[i].size) {
                // Check if extension is within direct data size
                if (new_size < DIRECT_DATA_SIZE) {
                    memset(filesystem[i].direct_data + filesystem[i].size, 0, new_size - filesystem[i].size);
                }
                // Optionally handle block data extension
            }
            filesystem[i].size = new_size;  // Update file size
            filesystem[i].mtime = time(NULL);
            filesystem[i].ctime = time(NULL);
            return 0;
        }
    }
    return -ENOENT; // File not found
}

/*
 * This is the only FUSE function that doesn't have a directly corresponding system call, although close(2) is related.
 * Release is called when FUSE is completely done with a file; at that point, you can free up any temporarily allocated data structures.
 */
int dm510fs_release(const char *path, struct fuse_file_info *fi) {
	printf("release: (path=%s)\n", path);
	return 0;
}

/**
 * Initialize filesystem
 *
 * The return value will passed in the `private_data` field of
 * `struct fuse_context` to all file operations, and as a
 * parameter to the destroy() method. It overrides the initial
 * value provided to fuse_main() / fuse_new().
 */
void* dm510fs_init() {
    FILE *fp = fopen(FS_STATE_FILE, "rb");
    if (fp) {
        // Existing file system state found, load it
        printf("Loading existing filesystem state.\n");
        fread(filesystem, sizeof(Inode), MAX_INODES, fp);
        fclose(fp);
    } else {
        // No existing state, initialize a new file system state
        printf("No existing state, initializing new filesystem.\n");
        for (int i = 0; i < MAX_INODES; i++) {
            filesystem[i].is_active = false;
        }
        // Initialize root directory
        filesystem[0].is_active = true;
        filesystem[0].is_dir = true;
        filesystem[0].mode = S_IFDIR | 0755;
        filesystem[0].nlink = 2;
        strcpy(filesystem[0].path, "/");

        // Initialize a sample file
        filesystem[1].is_active = true;
        filesystem[1].is_dir = false;
        filesystem[1].mode = S_IFREG | 0777;
        filesystem[1].nlink = 1;
        filesystem[1].size = 13; // "Hello World!\n" length including null terminator
        strcpy(filesystem[1].path, "/hello");
        memcpy(filesystem[1].direct_data, "Hello World!\n", 13);
    }
    return NULL; // No specific private data to pass back
}

/**
 * Clean up filesystem
 * Called on filesystem exit.
 */
void dm510fs_destroy(void *private_data) {
    printf("Cleaning up filesystem\n");
    save_fs_state(); // Save the filesystem state to the file
}


int main(int argc, char *argv[]) {
    return fuse_main(argc, argv, &dm510fs_oper);
}
