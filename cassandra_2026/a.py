
MX = 17


def get_partition(row):
    return row.__hash__() % MX


if __name__ == '__main__':
    w = (18, 'kadabra')
    print(w.__hash__())
    print(get_partition(w))

    g = (13, 'primavera')
    print(g.__hash__())
    print(get_partition(g))

    storage: list[list[tuple]] = [[] for _ in range(MX)]
    storage[get_partition(w)].append(w)
    storage[get_partition(g)].append(g)

    print(storage)

