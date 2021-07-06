import multiprocessing
import time

from runner import run, get_session


def get_data(configuration_file: str = "trades"):
    """
    @param configuration_file:
    @return:
    """
    print("entering new process")
    run(configuration_file)


def retrieve_data():
    while True:
        print("Hello from the Second Process")
        time.sleep(2)


def main():
    p1 = multiprocessing.Process(target=get_data)
    p2 = multiprocessing.Process(target=retrieve_data)
    p1.start()
    p2.start()


if __name__ == '__main__':
    main()