// object.c — Content-addressable object store

#include "pes.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <openssl/evp.h>

// ─── PROVIDED ────────────────────────────────────────────────────────────────

void hash_to_hex(const ObjectID *id, char *hex_out) {
    for (int i = 0; i < HASH_SIZE; i++) {
        sprintf(hex_out + i * 2, "%02x", id->hash[i]);
    }
    hex_out[HASH_HEX_SIZE] = '\0';
}

int hex_to_hash(const char *hex, ObjectID *id_out) {
    if (strlen(hex) < HASH_HEX_SIZE) return -1;
    for (int i = 0; i < HASH_SIZE; i++) {
        unsigned int byte;
        if (sscanf(hex + i * 2, "%2x", &byte) != 1) return -1;
        id_out->hash[i] = (uint8_t)byte;
    }
    return 0;
}

void compute_hash(const void *data, size_t len, ObjectID *id_out) {
    unsigned int hash_len;
    EVP_MD_CTX *ctx = EVP_MD_CTX_new();
    EVP_DigestInit_ex(ctx, EVP_sha256(), NULL);
    EVP_DigestUpdate(ctx, data, len);
    EVP_DigestFinal_ex(ctx, id_out->hash, &hash_len);
    EVP_MD_CTX_free(ctx);
}

void object_path(const ObjectID *id, char *path_out, size_t path_size) {
    char hex[HASH_HEX_SIZE + 1];
    hash_to_hex(id, hex);
    snprintf(path_out, path_size, "%s/%.2s/%s", OBJECTS_DIR, hex, hex + 2);
}

int object_exists(const ObjectID *id) {
    char path[512];
    object_path(id, path, sizeof(path));
    return access(path, F_OK) == 0;
}

// ─── IMPLEMENTED ─────────────────────────────────────────────────────────────

// Write object
int object_write(ObjectType type, const void *data, size_t len, ObjectID *id_out) {
    char header[64];
    const char *type_str;

    if (type == OBJ_BLOB) type_str = "blob";
    else if (type == OBJ_TREE) type_str = "tree";
    else type_str = "commit";

    int header_len = snprintf(header, sizeof(header), "%s %zu", type_str, len) + 1;

    size_t total_len = header_len + len;
    char *full = malloc(total_len);
    if (!full) return -1;

    memcpy(full, header, header_len);
    memcpy(full + header_len, data, len);

    compute_hash(full, total_len, id_out);

    if (object_exists(id_out)) {
        free(full);
        return 0;
    }

    // 🔥 IMPORTANT: Ensure base directory exists
    mkdir(".pes", 0755);
    mkdir(".pes/objects", 0755);

    char path[512];
    object_path(id_out, path, sizeof(path));

    // Extract directory
    char dir[512];
    strncpy(dir, path, sizeof(dir));
    char *slash = strrchr(dir, '/');
    if (slash) *slash = '\0';

    mkdir(dir, 0755);

    char temp_path[512];
    snprintf(temp_path, sizeof(temp_path), "%s/tmpXXXXXX", dir);

    int fd = mkstemp(temp_path);
    if (fd < 0) {
        free(full);
        return -1;
    }

    ssize_t written = write(fd, full, total_len);
    if (written != (ssize_t)total_len) {
        close(fd);
        unlink(temp_path);
        free(full);
        return -1;
    }

    fsync(fd);
    close(fd);

    if (rename(temp_path, path) != 0) {
        unlink(temp_path);
        free(full);
        return -1;
    }

    int dfd = open(dir, O_RDONLY);
    if (dfd >= 0) {
        fsync(dfd);
        close(dfd);
    }

    free(full);
    return 0;
}

// Read object
int object_read(const ObjectID *id, ObjectType *type_out, void **data_out, size_t *len_out) {
    char path[512];
    object_path(id, path, sizeof(path));

    FILE *fp = fopen(path, "rb");
    if (!fp) return -1;

    // File size
    fseek(fp, 0, SEEK_END);
    long size = ftell(fp);
    rewind(fp);

    if (size <= 0) {
        fclose(fp);
        return -1;
    }

    // Read file
    char *buffer = malloc(size);
    if (!buffer) {
        fclose(fp);
        return -1;
    }

    if (fread(buffer, 1, size, fp) != (size_t)size) {
        fclose(fp);
        free(buffer);
        return -1;
    }
    fclose(fp);

    // Verify hash
    ObjectID check;
    compute_hash(buffer, size, &check);
    if (memcmp(check.hash, id->hash, HASH_SIZE) != 0) {
        free(buffer);
        return -1;
    }

    // Find '\0'
    char *null_pos = memchr(buffer, '\0', size);
    if (!null_pos) {
        free(buffer);
        return -1;
    }

    // Parse header
    char type_str[10];
    size_t data_size;
    sscanf(buffer, "%s %zu", type_str, &data_size);

    if (strcmp(type_str, "blob") == 0) *type_out = OBJ_BLOB;
    else if (strcmp(type_str, "tree") == 0) *type_out = OBJ_TREE;
    else *type_out = OBJ_COMMIT;

    // Extract data
    char *data_start = null_pos + 1;

    *data_out = malloc(data_size);
    if (!*data_out) {
        free(buffer);
        return -1;
    }

    memcpy(*data_out, data_start, data_size);
    *len_out = data_size;

    free(buffer);
    return 0;
}
