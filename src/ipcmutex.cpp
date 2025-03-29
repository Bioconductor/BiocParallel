#define BOOST_NO_AUTO_PTR

#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include "cpp11.hpp"

static boost::uuids::random_generator uuid_generator;

std::string uuid_generate()
{
    return boost::uuids::to_string(uuid_generator());
}

#include "boost_patch_1_87/interprocess/managed_shared_memory.hpp"
#include <boost/interprocess/sync/interprocess_mutex.hpp>

using namespace boost::interprocess;

class IpcMutex
{

protected:

    managed_shared_memory *shm;

private:

    interprocess_mutex *mtx;
    bool *locked;

public:

    IpcMutex(const char *id) {
        shm = new managed_shared_memory{open_or_create, id, 1024};
        mtx = shm->find_or_construct<interprocess_mutex>("mtx")();
        locked = shm->find_or_construct<bool>("locked")();
    }

    ~IpcMutex() {
        delete shm;
    }

    bool is_locked() {
        return *locked;
    }

    bool lock() {
        mtx->lock();
        *locked = true;
        return *locked;
    }

    bool try_lock() {
        *locked = mtx->try_lock();
        return *locked;
    }

    bool unlock() {
        mtx->unlock();
        *locked = false;
        return *locked;
    }

};

class IpcCounter : IpcMutex
{

private:

    int *i;

public:

    IpcCounter(const char *id) : IpcMutex(id) {
        i = shm->find_or_construct<int>("i")();
    }

    ~IpcCounter() {}

    int value() {
        return *i + 1;
    }

    int reset(int n) {
        lock();
        *i = n - 1;
        unlock();
        return n;
    }

    int yield() {
        int result;
        lock();
        result = ++(*i);
        unlock();
        return result;
    }

};

#include <Rinternals.h>

// internal

const char *ipc_id(cpp11::strings id)
{
    if (id.size() != 1 || cpp11::is_na(id[0]) )
        Rf_error("'id' must be character(1) and not NA");
    return CHAR(static_cast<SEXP>(id[0]));
}

// utilities
[[cpp11::register]]
bool cpp_ipc_remove(cpp11::strings id_sexp) {
    const char *id = ipc_id(id_sexp);
    bool status = shared_memory_object::remove(id);
    return status;
}

// uuid
[[cpp11::register]]
cpp11::r_string cpp_ipc_uuid()
{
    std::string uuid = uuid_generate();
    return cpp11::r_string(uuid);
}

// mutex
[[cpp11::register]]
bool cpp_ipc_locked(cpp11::strings id_sexp)
{
    IpcMutex mutex = IpcMutex(ipc_id(id_sexp));
    bool status = mutex.is_locked();
    return status;
}

[[cpp11::register]]
bool cpp_ipc_lock(cpp11::strings id_sexp)
{
    IpcMutex mutex = IpcMutex(ipc_id(id_sexp));
    mutex.lock();
    return true;
}

[[cpp11::register]]
bool cpp_ipc_try_lock(cpp11::strings id_sexp)
{
    IpcMutex mutex = IpcMutex(ipc_id(id_sexp));
    bool status = mutex.try_lock();
    return status;
}

[[cpp11::register]]
bool cpp_ipc_unlock(cpp11::strings id_sexp)
{
    IpcMutex mutex = IpcMutex(ipc_id(id_sexp));
    bool status = mutex.unlock();
    return status;
}

// count
[[cpp11::register]]
int cpp_ipc_value(cpp11::strings id_sexp)
{
    IpcCounter cnt = IpcCounter(ipc_id(id_sexp));
    return cnt.value();
}

[[cpp11::register]]
int cpp_ipc_reset(cpp11::strings id_sexp, int n)
{
    IpcCounter cnt = IpcCounter(ipc_id(id_sexp));
    if (cpp11::is_na(n))
        Rf_error("'n' must not be NA");
    return cnt.reset(n);
}

[[cpp11::register]]
int cpp_ipc_yield(cpp11::strings id_sexp)
{
    IpcCounter cnt = IpcCounter(ipc_id(id_sexp));
    return cnt.yield();
}
