"""
Author: Oleg Shkolnik יב9.
Description: there are three classes for database in this program:
                first creates dictionary and has basic operations with it: set, get and delete value;

                second works with pickle, saving data in pkl file: loads and dumps data to/from this file;

                third creates semaphore and lock, writer and readers mutexes and make them follow the conditions:
                10 readers at the same time, one writer at the same time without any readers.
Date: 16/11/24
"""

import ctypes
from ctypes import wintypes
import pickle


# default file for database
filename = 'database.pkl'
get_fail = "This key is not in the database"


class BasicDataBase:
    def __init__(self):
        # creating dictionary from/in which will be loaded data in/from the file
        self.base = dict()

    def set_value(self, key, value):
        """
        checks if key isn't used, sets key with the value if it isn't
        :param key: key for database
        :param value: value for database
        :return: true/false if key is used in database
        """
        res = True
        if key in self.base:
            res = False
        else:
            self.base[key] = value
        return res

    def get_value(self, key):
        # looks for key in database, returns data that locates with the key or fail message
        result = get_fail
        if key in self.base:
            result = self.base[key]
        return result

    def delete_value(self, key):
        # checks if key is in the database and delete the data if it is
        if key in self.base:
            val = self.base[key]
            del self.base[key]
            return val


class PickleBase(BasicDataBase):
    global filename

    def __init__(self, data=filename):
        super().__init__()
        self.data = data

    def set_value(self, key, value):
        # function inherits 'basic database' function and also load result into pkl file
        while not self.load_from_file():
            continue
        res = super().set_value(key, value)
        while not self.save_to_file():
            continue
        return res

    def delete_value(self, key):
        # function inherits 'basic database' function and also load result into pkl file
        self.load_from_file()
        super().delete_value(key)
        while not self.save_to_file():
            continue

    def get_value(self, key):
        # function loads data from pkl file and then inherits 'basic database'
        while not self.load_from_file():
            continue
        return super().get_value(key)

    def save_to_file(self):
        # saving data into the file
        res = True
        try:
            with open(self.data, 'wb') as f:
                pickle.dump(self.base, f)
        except Exception as err:
            print(f'program got {err}')
            res = False
        finally:
            return res

    def load_from_file(self):
        # loading data from the file
        res = True
        try:
            with open(self.data, 'rb') as f:
                self.base = pickle.load(f)
        except Exception as err:
            print(f'program got {err}')
            res = False
        finally:
            return res


class DataBase(PickleBase):

    def __init__(self, db=filename):
        super().__init__(db)

        self.reader_count = 0

        # WinAPI constants for named objects
        semaphore_name = "Global\\DatabaseReaderSemaphore"
        mutex_name = "Global\\DatabaseWriterMutex"

        # Create a named semaphore with a maximum of 10 simultaneous readers
        self.kernel32 = ctypes.windll.kernel32

        self.reader_semaphore = self.kernel32.CreateSemaphoreW(None, 10, 10, semaphore_name)
        if not self.reader_semaphore:
            raise ctypes.WinError()

        # Create a named mutex to ensure exclusive write access
        self.writer_mutex = self.kernel32.CreateMutexW(None, False, mutex_name)
        if not self.writer_mutex:
            raise ctypes.WinError()

    def acquire_reader_access(self):
        """
        Acquires access for a reader:
        Waits for a slot in the semaphore.
        If this is the first reader, locks the writer mutex to prevent writing.
        """

        self.kernel32.WaitForSingleObject(self.reader_semaphore, wintypes.DWORD(-1))

        self.reader_count += 1
        if self.reader_count == 1:
            self.kernel32.WaitForSingleObject(self.writer_mutex, wintypes.DWORD(-1))

    def release_reader_access(self):
        """
        Releases access for a reader.
        Releases the writer mutex to allow writing.
        Frees up a slot in the semaphore for other readers.
        """
        self.reader_count -= 1
        if self.reader_count == 0:
            self.kernel32.ReleaseMutex(self.writer_mutex)

        self.kernel32.ReleaseSemaphore(self.reader_semaphore, 1, None)

    def acquire_writer_access(self):
        """
        Acquires access for a writer.
        Waits for the writer mutex to ensure write access.
        Blocks all readers until the mutex is released.
        """
        self.kernel32.WaitForSingleObject(self.writer_mutex, wintypes.DWORD(-1))

    def release_writer_access(self):
        """
        Releases access for a writer.
        Unlocks the writer mutex, allowing readers or other writers to proceed.
        """
        self.kernel32.ReleaseMutex(self.writer_mutex)

    def set_value(self, key, value):
        # setting the value
        self.acquire_writer_access()
        try:
            res = super().set_value(key, value)
        finally:
            self.release_writer_access()
        return res

    def delete_value(self, key):
        # deleting the value
        self.acquire_writer_access()
        try:
            super().delete_value(key)
        finally:
            self.release_writer_access()

    def get_value(self, key):
        # getting the value
        self.acquire_reader_access()
        try:
            return super().get_value(key)
        finally:
            self.release_reader_access()


if __name__ == '__main__':
    login = 1000
    password = 1000

    test_db1 = BasicDataBase()
    assert test_db1.set_value(login, password) == True
    assert test_db1.get_value(login) == password
    test_db1.delete_value(login)
    assert test_db1.get_value(login) != password

    test_db2 = PickleBase()
    assert test_db2.set_value(login, password) == True
    assert test_db2.get_value(login) == password
    test_db2.delete_value(login)
    assert test_db2.get_value(login) != password

    test_db3 = DataBase()
    assert test_db3.set_value(login, password) == True
    assert test_db3.get_value(login) == password
    test_db3.delete_value(login)
    assert test_db3.get_value(login) != password

    test_db4 = DataBase()
    assert test_db4.set_value(login, password) == True
    assert test_db4.get_value(login) == password
    test_db4.delete_value(login)
    assert test_db4.get_value(login) != password
