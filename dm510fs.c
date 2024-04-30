#include <fuse.h>
#include <errno.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdbool.h>
#include <time.h>
#include <utime.h>
#include <sys/types.h>

int dm510fs_getattr( const char *, struct stat * );
int dm510fs_readdir( const char *, void *, fuse_fill_dir_t, off_t, struct fuse_file_info * );
int dm510fs_open( const char *, struct fuse_file_info * );
int dm510fs_read( const char *, char *, size_t, off_t, struct fuse_file_info * );
int dm510fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi);
int dm510fs_release(const char *path, struct fuse_file_info *fi);
int dm510fs_mkdir(const char *path, mode_t mode);
int dm510fs_mknod(const char *path, mode_t  mode, dev_t rdev);
int dm510fs_unlink(const char *path);
int dm510fs_rmdir(const char *path);
int dm510fs_utime(const char *path, struct utimbuf *ubuf);
int dm510fs_rename(const char *oldpath, const char *newpath);
int dm510fs_truncate(const char *path, off_t size);
void* dm510fs_init();
void dm510fs_destroy(void *private_data);

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
  // Path to save filesystem state


/* The Inode for the filesystem*/
typedef struct Inode {
	bool is_active;     // Indicates whether the indoe is in use
	bool is_dir;        // Indicates whether the indoe is a directory
	char data[MAX_DATA_IN_FILE];    // Fixed size array, stores the files data
	char path[MAX_PATH_LENGTH];     // A string storing the full path of the file/directory
	char name[MAX_NAME_LENGTH];     // String representing the name of the file/directory 
	mode_t mode;        // Specify the file type and permissions
	nlink_t nlink;      // The link count
	off_t size;         // Size of the file in bytes
	time_t atime;       // Access time
	time_t mtime;       // Modificaton time
	time_t ctime;       //Change time
} Inode;

// Array that represent the entire filesystem 
Inode filesystem[MAX_INODES];

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
    (void) offset; // Offset is handled by FUSE itself
    (void) fi;     // Not used

    printf("readdir: (path=%s)\n", path);

    // Checks if the path is the root directory
    if (strcmp(path, "/") != 0) {
        return -ENOENT;
    }

    filler(buf, ".", NULL, 0);  // Current directory
    filler(buf, "..", NULL, 0); // Parent directory

    // Loop through all inodes and list active files and directories
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active) {
            if (filesystem[i].path[0] == '/' && strcmp(filesystem[i].path, "/") != 0) { // Avoid listing root itself
                const char* name = filesystem[i].path + 1; // Skip the leading '/'
                filler(buf, name, NULL, 0);
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
int dm510fs_read( const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi ) {
    printf("read: (path=%s)\n", path);

	for( int i = 0; i < MAX_INODES; i++) {
		if( strcmp(filesystem[i].path, path) == 0 ) {
			printf("Read: Found inode for path %s at location %i\n", path, i);
			memcpy( buf, filesystem[i].data, filesystem[i].size );
			return filesystem[i].size;
		}
	}
	return 0;
}

// Writes data to a file
int dm510fs_write(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi) {
    (void) fi; // If you're not using file handles.

    // Find the inode corresponding to the path.
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0) {
            // Make sure we don't write past the file's maximum data size.
            if (offset + size > MAX_DATA_IN_FILE) {
                size = MAX_DATA_IN_FILE - offset;
            }
            if (size == 0) {
                return -EFBIG; // File too big.
            }

            // Perform the write operation.
            memcpy(filesystem[i].data + offset, buf, size);

            // Update the size of the file.
            if (offset + size > filesystem[i].size) {
                filesystem[i].size = offset + size;
            }

            // Return the number of bytes written.
            return size;
        }
    }

    // If the file was not found, return an error.
    return -ENOENT;
}

/* Make directories - TODO */
int dm510fs_mkdir(const char *path, mode_t mode) {
	printf("mkdir: (path=%s)\n", path);

	// Locate the first unused Inode in the filesystem
	for( int i = 0; i < MAX_INODES; i++) {
		if( filesystem[i].is_active == false ) {
			printf("mkdir: Found unused inode for at location %i\n", i);
			// Use that for the directory
			filesystem[i].is_active = true;
			filesystem[i].is_dir = true;
			filesystem[i].mode = S_IFDIR | 0755;
			filesystem[i].nlink = 2;
			memcpy(filesystem[i].path, path, strlen(path)+1); 	

			debug_inode(i);
			break;
		}
	}

	return 0;
}

// Make files 
int dm510fs_mknod(const char *path, mode_t mode, dev_t rdev) {
    printf("mknod: (path=%s, mode=%o)\n", path, mode);

    // Check if the path already exists
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0) {
            printf("mknod: File already exists\n");
            return -EEXIST;
        }
    }

    // Find an inactive inode
    int inode_index = -1;
    for (int i = 0; i < MAX_INODES; i++) {
        if (!filesystem[i].is_active) {
            inode_index = i;
            break;
        }
    }

    if (inode_index == -1) {
        printf("mknod: No available inode\n");
        return -ENOSPC;  // No space left on device
    }

    // Initialize the inode
    Inode *inode = &filesystem[inode_index];
    inode->is_active = true;
    inode->is_dir = false;
    inode->mode = mode;
    inode->nlink = 1;
    inode->size = 0;  // Initially, the size is 0 because no data is written yet
    strncpy(inode->path, path, MAX_PATH_LENGTH);
    memset(inode->data, 0, MAX_DATA_IN_FILE);

    printf("mknod: File created at inode %d\n", inode_index);
    return 0;
}



// Remove a file.
int dm510fs_unlink(const char *path) {
    printf("unlink: (path=%s)\n", path);

    // Loop through the inode array to find and remove the file
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0) {
            // File found, now deactivate the inode
            filesystem[i].is_active = false;
            filesystem[i].size = 0;  // Optionally reset the size
            memset(filesystem[i].data, 0, MAX_DATA_IN_FILE);  // Optionally clear the data

            printf("Unlink: Successfully removed %s at location %i\n", path, i);
            return 0;  // Success
        }
    }

    // If we finish the loop without finding the file, it doesn't exist
    printf("Unlink: File %s not found\n", path);
    return -ENOENT;  // No such file
}


// Remove a directory.
int dm510fs_rmdir(const char *path) {
    printf("rmdir: (path=%s)\n", path);

    // First, checks if the directory is at the root and is "/", which should not be removed
    if (strcmp(path, "/") == 0) {
        printf("Cannot remove root directory\n");
        return -EBUSY; // or return -EPERM
    }

    // Locates the inode representing the directory
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0 && filesystem[i].is_dir) {
            // Check if the directory is empty
            bool is_empty = true;
            for (int j = 0; j < MAX_INODES; j++) {
                if (filesystem[j].is_active && strncmp(filesystem[j].path, path, strlen(path)) == 0 && j != i) {
                    is_empty = false;
                    break;
                }
            }

            if (!is_empty) {
                printf("Directory is not empty\n");
                return -ENOTEMPTY; // Directory not empty
            }

            // If the directory is empty, deactivate the inode
            filesystem[i].is_active = false;
            memset(&filesystem[i], 0, sizeof(Inode)); 
            printf("Directory removed successfully\n");
            return 0; // Success
        }
    }

    // If no matching directory is found
    printf("No such directory\n");
    return -ENOENT; // No such directory
}


// Renames a file or directory.
int dm510fs_rename(const char *oldpath, const char *newpath) {
    printf("rename: (oldpath=%s, newpath=%s)\n", oldpath, newpath);

    int i, target_idx = -1;

    // Checks if the new path already exists
    for (i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, newpath) == 0) {
            target_idx = i;
            break;
        }
    }

    // Finds the inode for the old path
    for (i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, oldpath) == 0) {
            // If the new path exists and is a non-empty directory, return error
            if (target_idx != -1 && filesystem[target_idx].is_dir) {
                printf("rename: Target is a non-empty directory\n");
                return -ENOTEMPTY;
            }

            // If it's an existing file, deactivate it
            if (target_idx != -1) {
                filesystem[target_idx].is_active = false;
            }

            // Update the inode with the new path
            strncpy(filesystem[i].path, newpath, MAX_PATH_LENGTH);
            printf("rename: Successfully renamed %s to %s\n", oldpath, newpath);
            return 0;
        }
    }

    printf("rename: Old path does not exist\n");
    return -ENOENT;
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
    printf("truncate: (path=%s, new_size=%lld)\n", path, (long long)new_size);

    // Finds the inode for the given path
    for (int i = 0; i < MAX_INODES; i++) {
        if (filesystem[i].is_active && strcmp(filesystem[i].path, path) == 0) {
            if (new_size < filesystem[i].size) {
                // Shrinks the file, so clear the truncated part
                memset(filesystem[i].data + new_size, 0, filesystem[i].size - new_size);
            } else if (new_size > filesystem[i].size) {
                // Extending the file, so fill the new space with zeroes
                if (new_size > MAX_DATA_IN_FILE) {
                    // New size is too large for our file data array
                    return -EFBIG;
                }
                memset(filesystem[i].data + filesystem[i].size, 0, new_size - filesystem[i].size);
            }

            // Set the new file size
            filesystem[i].size = new_size;

            // Update the inode modification time to the current time
            filesystem[i].mtime = time(NULL);

            // Change update ctime as well
            filesystem[i].ctime = filesystem[i].mtime;

            return 0; // Success
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
        // File exists, load state
        printf("Loading existing filesystem state.\n");
        fread(filesystem, sizeof(Inode), MAX_INODES, fp);
        fclose(fp);
    } else {
        // File does not exist, initialize default state
        printf("No existing state, initializing new filesystem.\n");
        for (int i = 0; i < MAX_INODES; i++) {
            filesystem[i].is_active = false;
        }
        filesystem[0].is_active = true;
        filesystem[0].is_dir = true;
        filesystem[0].mode = S_IFDIR | 0755;
        filesystem[0].nlink = 2;
        strcpy(filesystem[0].path, "/");

        filesystem[1].is_active = true;
        filesystem[1].is_dir = false;
        filesystem[1].mode = S_IFREG | 0777;
        filesystem[1].nlink = 1;
        filesystem[1].size = 13; // "Hello World!\n" length including null terminator
        strcpy(filesystem[1].path, "/hello");
        strcpy(filesystem[1].data, "Hello World!\n");
    }
    return NULL;
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
