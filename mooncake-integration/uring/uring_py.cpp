// Copyright 2025 KVCache.AI
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// mooncake.uring: Python bindings for the io_uring file backend shared by
// Mooncake Store, vLLM's disk offload and SGLang's HiCache client.

#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

#include <fcntl.h>
#include <unistd.h>

#include <cerrno>
#include <climits>
#include <cstdint>
#include <cstring>
#include <memory>
#include <string>
#include <vector>

#ifdef USE_URING
#include "file_interface.h"
#endif

namespace py = pybind11;

namespace {

// Matches UringFile::ALIGNMENT_, the alignment O_DIRECT requires for buffer
// addresses, lengths and offsets.
constexpr size_t kDirectIoAlignment = 4096;

// One io_uring request carries a 32-bit byte count and the kernel caps it at
// MAX_RW_COUNT. read_aligned/write_aligned chunk larger transfers; batch
// descriptors are submitted as-is, so their size is validated here.
constexpr size_t kMaxBatchRequestBytes =
    static_cast<size_t>(INT_MAX) & ~(kDirectIoAlignment - 1);

[[noreturn]] void throwOsError(int err, const std::string& what,
                               const std::string& filename) {
    py::object args = filename.empty() ? py::make_tuple(err, what)
                                       : py::make_tuple(err, what, filename);
    PyErr_SetObject(PyExc_OSError, args.ptr());
    throw py::error_already_set();
}

#ifdef USE_URING
using mooncake::ErrorCode;
using mooncake::UringFile;

// Positive errno for a UringFile failure. Argument validation maps to
// EINVAL/EBADF; ring-level failures carry the errno of the failed completion
// when one was recorded and EIO otherwise.
int errnoFromStatus(ErrorCode code, int io_errno) {
    switch (code) {
        case ErrorCode::OK:
            return 0;
        case ErrorCode::FILE_INVALID_BUFFER:
            return EINVAL;
        case ErrorCode::FILE_NOT_FOUND:
        case ErrorCode::FILE_INVALID_HANDLE:
            return EBADF;
        default:
            return io_errno > 0 ? io_errno : EIO;
    }
}

size_t batchBytes(const UringFile::ReadDesc& desc) { return desc.bytes_read; }
size_t batchBytes(const UringFile::WriteDesc& desc) {
    return desc.bytes_written;
}
#endif

class UringFilePy {
   public:
    UringFilePy(const std::string& path, int flags, unsigned queue_depth,
                bool direct_io, int mode)
        : path_(path), direct_io_(direct_io) {
#ifndef USE_URING
        (void)flags;
        (void)queue_depth;
        (void)mode;
        throwOsError(ENOSYS, "mooncake.uring was built without liburing", path);
#else
        int ring_errno = UringFile::thread_ring_errno();
        if (ring_errno) {
            throwOsError(ring_errno,
                         std::string("io_uring is unavailable: ") +
                             std::strerror(ring_errno),
                         path);
        }
        const int open_flags = direct_io ? (flags | O_DIRECT) : flags;
        int fd = -1;
        int open_errno = 0;
        {
            py::gil_scoped_release release;
            fd = ::open(path.c_str(), open_flags, mode);
            if (fd < 0) open_errno = errno;
        }
        if (fd < 0) throwOsError(open_errno, std::strerror(open_errno), path);
        file_ = std::make_unique<UringFile>(path, fd, queue_depth, direct_io);
        // The store unlinks its own arena files after a failed write; a
        // caller-owned file must never disappear.
        file_->SetDeleteOnWriteFail(false);
        fd_ = fd;
#endif
    }

    ~UringFilePy() { close(); }

    UringFilePy(const UringFilePy&) = delete;
    UringFilePy& operator=(const UringFilePy&) = delete;

    int64_t readAligned(uintptr_t buf_ptr, size_t length, int64_t offset) {
        return submitOne(false, buf_ptr, length, offset);
    }

    int64_t writeAligned(uintptr_t buf_ptr, size_t length, int64_t offset) {
        return submitOne(true, buf_ptr, length, offset);
    }

    std::vector<int64_t> batchRead(const std::vector<uintptr_t>& buf_ptrs,
                                   const std::vector<size_t>& lengths,
                                   const std::vector<int64_t>& offsets) {
#ifdef USE_URING
        return submitBatch<UringFile::ReadDesc>(
            buf_ptrs, lengths, offsets,
            [](UringFile& file, UringFile::ReadDesc* descs, int cnt) {
                return file.batch_read(descs, cnt);
            });
#else
        return unsupportedBatch(buf_ptrs, lengths, offsets);
#endif
    }

    std::vector<int64_t> batchWrite(const std::vector<uintptr_t>& buf_ptrs,
                                    const std::vector<size_t>& lengths,
                                    const std::vector<int64_t>& offsets) {
#ifdef USE_URING
        return submitBatch<UringFile::WriteDesc>(
            buf_ptrs, lengths, offsets,
            [](UringFile& file, UringFile::WriteDesc* descs, int cnt) {
                return file.batch_write(descs, cnt);
            });
#else
        return unsupportedBatch(buf_ptrs, lengths, offsets);
#endif
    }

    int datasync() {
#ifndef USE_URING
        return -ENOSYS;
#else
        if (!file_) return -EBADF;
        py::gil_scoped_release release;
        auto result = file_->datasync();
        if (result) return 0;
        return -errnoFromStatus(result.error(), UringFile::last_io_errno());
#endif
    }

    void close() {
#ifdef USE_URING
        if (!file_) return;
        std::unique_ptr<UringFile> file;
        file.swap(file_);
        fd_ = -1;
        py::gil_scoped_release release;
        file.reset();  // UringFile's destructor closes the descriptor.
#endif
    }

    int fileno() const { return fd_; }
    bool closed() const { return fd_ < 0; }
    const std::string& path() const { return path_; }
    bool directIo() const { return direct_io_; }

   private:
    int64_t submitOne(bool is_write, uintptr_t buf_ptr, size_t length,
                      int64_t offset) {
#ifndef USE_URING
        (void)is_write;
        (void)buf_ptr;
        (void)length;
        (void)offset;
        return -ENOSYS;
#else
        if (!file_) return -EBADF;
        if (offset < 0) return -EINVAL;
        void* buf = reinterpret_cast<void*>(buf_ptr);
        py::gil_scoped_release release;
        auto result = is_write ? file_->write_aligned(buf, length, offset)
                               : file_->read_aligned(buf, length, offset);
        if (result) return static_cast<int64_t>(result.value());
        return -errnoFromStatus(result.error(), UringFile::last_io_errno());
#endif
    }

    static void checkBatchShape(size_t buf_count, size_t len_count,
                                size_t off_count) {
        if (buf_count != len_count || buf_count != off_count) {
            throw py::value_error(
                "buf_ptrs, lengths and offsets must have the same length");
        }
    }

#ifdef USE_URING
    // Returns one entry per descriptor: bytes transferred, or -errno. A
    // validation failure means nothing was submitted and every entry reports
    // -EINVAL (or -EBADF on a closed file).
    template <typename Desc, typename Submit>
    std::vector<int64_t> submitBatch(const std::vector<uintptr_t>& buf_ptrs,
                                     const std::vector<size_t>& lengths,
                                     const std::vector<int64_t>& offsets,
                                     Submit submit) {
        checkBatchShape(buf_ptrs.size(), lengths.size(), offsets.size());
        const size_t count = buf_ptrs.size();
        if (count == 0) return {};
        if (!file_) return std::vector<int64_t>(count, -EBADF);

        std::vector<Desc> descs;
        descs.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            if (offsets[i] < 0 || lengths[i] > kMaxBatchRequestBytes) {
                return std::vector<int64_t>(count, -EINVAL);
            }
            descs.push_back(Desc{reinterpret_cast<void*>(buf_ptrs[i]),
                                 lengths[i], static_cast<off_t>(offsets[i])});
        }

        int io_errno = 0;
        auto status = [&] {
            py::gil_scoped_release release;
            auto result = submit(*file_, descs.data(), static_cast<int>(count));
            io_errno = UringFile::last_io_errno();
            return result;
        }();
        const int batch_errno =
            status ? 0 : errnoFromStatus(status.error(), io_errno);

        std::vector<int64_t> results(count, 0);
        for (size_t i = 0; i < count; ++i) {
            const Desc& desc = descs[i];
            if (!desc.completed) {
                results[i] = -(batch_errno ? batch_errno : EIO);
            } else if (desc.error != ErrorCode::OK) {
                results[i] = -errnoFromStatus(desc.error, desc.sys_errno);
            } else {
                results[i] = static_cast<int64_t>(batchBytes(desc));
            }
        }
        return results;
    }
#else
    std::vector<int64_t> unsupportedBatch(
        const std::vector<uintptr_t>& buf_ptrs,
        const std::vector<size_t>& lengths,
        const std::vector<int64_t>& offsets) {
        checkBatchShape(buf_ptrs.size(), lengths.size(), offsets.size());
        return std::vector<int64_t>(buf_ptrs.size(), -ENOSYS);
    }
#endif

    std::string path_;
    bool direct_io_;
    int fd_ = -1;
#ifdef USE_URING
    std::unique_ptr<UringFile> file_;
#endif
};

bool registerGlobalBuffer(uintptr_t ptr, size_t length) {
#ifdef USE_URING
    py::gil_scoped_release release;
    return UringFile::register_global_buffer(reinterpret_cast<void*>(ptr),
                                             length);
#else
    (void)ptr;
    (void)length;
    return false;
#endif
}

void unregisterGlobalBuffer() {
#ifdef USE_URING
    py::gil_scoped_release release;
    UringFile::unregister_global_buffer();
#endif
}

}  // namespace

PYBIND11_MODULE(uring, m) {
    m.doc() =
        "io_uring file I/O shared by Mooncake Store, vLLM and SGLang. "
        "I/O methods return the byte count on success and -errno on failure; "
        "constructor failures raise OSError.";

#ifdef USE_URING
    m.attr("SUPPORT_URING") = true;
#else
    m.attr("SUPPORT_URING") = false;
#endif
    m.attr("DIRECT_IO_ALIGNMENT") = py::int_(kDirectIoAlignment);

    py::class_<UringFilePy>(
        m, "UringFile",
        "A file accessed through the calling thread's io_uring ring. Each "
        "thread lazily creates one ring of depth 32 that all UringFile "
        "instances on that thread share.")
        .def(py::init<const std::string&, int, unsigned, bool, int>(),
             py::arg("path"), py::arg("flags"), py::arg("queue_depth") = 32,
             py::arg("direct_io") = false, py::arg("mode") = 0644,
             "Open `path` with os.open-style `flags` (O_RDWR | O_CREAT, ...). "
             "`direct_io` adds O_DIRECT, after which buffer addresses, lengths "
             "and offsets must be multiples of DIRECT_IO_ALIGNMENT. "
             "`queue_depth` is accepted for API compatibility; the shared "
             "per-thread ring has a fixed depth. Raises OSError when the file "
             "cannot be opened or io_uring is unavailable.")
        .def("read_aligned", &UringFilePy::readAligned, py::arg("buf_ptr"),
             py::arg("length"), py::arg("offset"),
             "Read `length` bytes at `offset` into the memory at `buf_ptr`. "
             "Returns the bytes read (0 at end of file) or -errno.")
        .def("write_aligned", &UringFilePy::writeAligned, py::arg("buf_ptr"),
             py::arg("length"), py::arg("offset"),
             "Write `length` bytes from `buf_ptr` at `offset`. Returns the "
             "bytes written or -errno.")
        .def("batch_read", &UringFilePy::batchRead, py::arg("buf_ptrs"),
             py::arg("lengths"), py::arg("offsets"),
             "Submit independent reads in rounds of the ring depth and wait "
             "for all of them. Returns one entry per descriptor: bytes read "
             "or -errno. Descriptors inside the registered buffer use "
             "fixed-buffer I/O.")
        .def("batch_write", &UringFilePy::batchWrite, py::arg("buf_ptrs"),
             py::arg("lengths"), py::arg("offsets"),
             "Write-side counterpart of batch_read.")
        .def("datasync", &UringFilePy::datasync,
             "Flush data with IORING_FSYNC_DATASYNC. Returns 0 or -errno.")
        .def("close", &UringFilePy::close,
             "Close the file; later I/O calls return -EBADF. Idempotent.")
        .def("fileno", &UringFilePy::fileno,
             "The underlying descriptor, or -1 after close().")
        .def_property_readonly("closed", &UringFilePy::closed)
        .def_property_readonly("path", &UringFilePy::path)
        .def_property_readonly("direct_io", &UringFilePy::directIo)
        .def("__enter__", [](py::object self) { return self; })
        .def("__exit__", [](UringFilePy& self, py::object, py::object,
                            py::object) { self.close(); });

    m.def("register_global_buffer", &registerGlobalBuffer, py::arg("ptr"),
          py::arg("length"),
          "Register [ptr, ptr + length) as the process-wide io_uring fixed "
          "buffer (UringFile::register_global_buffer). Call once from a single "
          "thread before I/O threads start; every thread's ring picks the "
          "registration up on its first I/O. Returns False when the kernel "
          "refuses the registration, in which case I/O still works without "
          "fixed buffers.");
    m.def("unregister_global_buffer", &unregisterGlobalBuffer,
          "Clear the process-wide registration and unregister it from the "
          "calling thread's ring.");
}
