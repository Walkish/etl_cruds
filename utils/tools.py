from os.path import isfile, join, splitext
from os import listdir


def list_all_files_in_dir(src_folder_path: str, extension: str = None):
    """
    :param src_folder_path: "/src"
    :param extension:  ".gz" , ".txt"
    :return: List[str]
    """

    if extension:
        return [
            f
            for f in listdir(src_folder_path)
            if splitext(f)[1] == extension and isfile(join(src_folder_path, f))
        ]
    else:
        return listdir(src_folder_path)
