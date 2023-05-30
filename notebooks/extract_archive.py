# SuperFastPython.com
# unzip a large number of files concurrently with processes in batch
from zipfile import ZipFile
from tarfile import TarFile
from concurrent.futures import ProcessPoolExecutor

# unzip files from an archive


def unzip_files(tar_filename, filenames, path):
    # open the zip file
    with TarFile(tar_filename, 'r') as handle:
        # unzip a batch of files
        handle.extractall(path=path, members=filenames)

# unzip a large number of files


def main(path='tmp', tar_filename='testing.zip'):
    # open the zip file
    with TarFile(tar_filename, 'r') as handle:
        # list of all files to unzip
        files = handle.namelist()
    # determine chunksize
    n_workers = 8
    chunksize = round(len(files) / n_workers)
    # start the thread pool
    with ProcessPoolExecutor(n_workers) as exe:
        # split the copy operations into chunks
        for i in range(0, len(files), chunksize):
            # select a chunk of filenames
            filenames = files[i:(i + chunksize)]
            # submit the batch copy task
            _ = exe.submit(unzip_files, tar_filename, filenames, path)


# entry point
if __name__ == '__main__':
    main()
