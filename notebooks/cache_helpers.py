import os


def check_cache(filename='temp.csv'):
    return os.path.exists(os.path.join('data', filename))


def write_cache(data: list[str], filename='temp.csv'):
    try:
        with open(os.path.join('data', filename), 'w') as f:
            for x in data:
                f.write(x+"\n")
    except FileNotFoundError as e:
        print('cache not hit')


def read_cache(filename='temp.csv'):
    with open(os.path.join('data', filename), 'r') as f:
        try:
            # Create an empty list to store the lines
            lines = []

            # Iterate over the lines of the file
            for line in f:
                # Remove the newline character at the end of the line
                line = line.strip()

                # Append the line to the list
                lines.append(line)
            lines.reverse()
            return lines
        except Exception as e:
            f.close()
            raise e
