"""ЛР №8: MPI. Коллективные операции, группы/коммуникаторы и файловый ввод-вывод.

Расширяет ЛР №7:
  1. Коллективные операции (Bcast / Scatterv / Gatherv) вместо парных.
  2. Произвольное число групп (--groups) со случайным числом процессов в каждой.
     Каждая группа независимо умножает матрицы, замеряется время по группам.
  3. Файловые операции (MPI-IO): исходные матрицы читаются из 2 общих файлов;
     каждый процесс читает свою полосу строк по своей координате (rank),
     а результат пишет в файл своей группы.

Итоговое время по группам сравнивается со временем парных операций (ЛР №7).

Подготовка общих файлов и запуск (файлы должны лежать на общем для узлов ФС):
    python3 lab8/matmul_groups.py --make-input --size 1200 --dir ./shared
    mpirun --hostfile hosts -np 8 python3 lab8/matmul_groups.py \
        --size 1200 --groups 3 --dir ./shared
"""
import argparse
import os
import sys

import numpy as np
from mpi4py import MPI

_LAB7 = os.path.join(os.path.dirname(__file__), "..", "lab7")
sys.path.insert(0, os.path.abspath(_LAB7))
import matmul as lab7  # переиспользуем парные операции и разбиение строк (ЛР7)


def plan_groups(world_size: int, n_groups: int, seed: int) -> list:
    """Случайно делит процессы на n_groups непустых групп (одинаково на всех рангах)."""
    rng = np.random.default_rng(seed)
    if n_groups > world_size:
        n_groups = world_size
    cuts = rng.choice(range(1, world_size), size=n_groups - 1, replace=False)
    bounds = [0, *sorted(int(c) for c in cuts), world_size]
    colors = []
    for gid in range(n_groups):
        colors += [gid] * (bounds[gid + 1] - bounds[gid])
    return colors


def make_input_files(dir_path: str, n: int, seed: int) -> None:
    """Создаёт два общих файла с матрицами A и B (сырой float64, n×n)."""
    os.makedirs(dir_path, exist_ok=True)
    lab7.make_matrix(n, seed).tofile(os.path.join(dir_path, "matrixA.bin"))
    lab7.make_matrix(n, seed + 1).tofile(os.path.join(dir_path, "matrixB.bin"))
    print(f"созданы matrixA.bin и matrixB.bin размера {n}×{n} в {dir_path}")


def collective_multiply(comm, n, a, b):
    """Умножение через коллективы: Bcast(B) + Scatterv(A) + Gatherv(C)."""
    ranges = lab7.row_ranges(n, comm.Get_size())
    b_local = _bcast_b(comm, b, n)
    a_block = _scatter_rows(comm, a, n, ranges)
    c_block = np.ascontiguousarray(a_block @ b_local)
    return _gather_rows(comm, c_block, n, ranges)


def _bcast_b(comm, b, n):
    """Рассылает матрицу B всем процессам коммуникатора."""
    buf = np.ascontiguousarray(b) if comm.Get_rank() == 0 else np.empty((n, n), np.float64)
    comm.Bcast([buf, MPI.DOUBLE], root=0)
    return buf


def _scatter_rows(comm, a, n, ranges):
    """Раздаёт полосы строк A: каждому процессу — свой блок (Scatterv)."""
    counts = [(e - s) * n for s, e in ranges]
    displs = [s * n for s, _ in ranges]
    rows = ranges[comm.Get_rank()][1] - ranges[comm.Get_rank()][0]
    recv = np.empty((rows, n), np.float64)
    sendbuf = [np.ascontiguousarray(a), counts, displs, MPI.DOUBLE] if comm.Get_rank() == 0 else None
    comm.Scatterv(sendbuf, [recv, MPI.DOUBLE], root=0)
    return recv


def _gather_rows(comm, c_block, n, ranges):
    """Собирает полосы результата в матрицу C на процессе 0 (Gatherv)."""
    counts = [(e - s) * n for s, e in ranges]
    displs = [s * n for s, _ in ranges]
    c = np.empty((n, n), np.float64) if comm.Get_rank() == 0 else None
    recvbuf = [c, counts, displs, MPI.DOUBLE] if comm.Get_rank() == 0 else None
    comm.Gatherv([c_block, MPI.DOUBLE], recvbuf, root=0)
    return c


def _read_band(comm, path, n, my_range):
    """Читает свою полосу строк из общего файла по координате (MPI-IO Read_at_all)."""
    s, e = my_range
    rows = e - s
    band = np.empty((rows, n), np.float64)
    fh = MPI.File.Open(comm, path, MPI.MODE_RDONLY)
    fh.Read_at_all(s * n * 8, [band, MPI.DOUBLE])
    fh.Close()
    return band


def _read_full(comm, path, n):
    """Читает всю матрицу B из общего файла (одинаково всеми процессами)."""
    buf = np.empty((n, n), np.float64)
    fh = MPI.File.Open(comm, path, MPI.MODE_RDONLY)
    fh.Read_at_all(0, [buf, MPI.DOUBLE])
    fh.Close()
    return buf


def _write_band(comm, path, n, my_range, band):
    """Пишет свою полосу результата в файл группы по координате (MPI-IO Write_at_all)."""
    s, _ = my_range
    fh = MPI.File.Open(comm, path, MPI.MODE_WRONLY | MPI.MODE_CREATE)
    fh.Write_at_all(s * n * 8, [np.ascontiguousarray(band), MPI.DOUBLE])
    fh.Close()


def group_multiply_files(group, gid, n, dir_path):
    """Группа читает матрицы из файлов, умножает и пишет результат в свой файл."""
    ranges = lab7.row_ranges(n, group.Get_size())
    my = ranges[group.Get_rank()]
    group.Barrier()
    t0 = MPI.Wtime()
    a_band = _read_band(group, os.path.join(dir_path, "matrixA.bin"), n, my)
    b_full = _read_full(group, os.path.join(dir_path, "matrixB.bin"), n)
    c_band = a_band @ b_full
    _write_band(group, os.path.join(dir_path, f"group_{gid}_C.bin"), n, my, c_band)
    return group.allreduce(MPI.Wtime() - t0, op=MPI.MAX)


def _pairwise_baseline(comm, n, seed):
    """Время парных операций (ЛР7) на всём мире — для сравнения."""
    a = lab7.make_matrix(n, seed) if comm.Get_rank() == 0 else None
    b = lab7.make_matrix(n, seed + 1) if comm.Get_rank() == 0 else None
    _c, dt = lab7.run_blocking(comm, a, b, n)
    return dt


def build_parser() -> argparse.ArgumentParser:
    """Разбор аргументов."""
    p = argparse.ArgumentParser(description="MPI группы, коллективы, файлы (ЛР8)")
    p.add_argument("--size", type=int, default=1200, help="размер матрицы n×n")
    p.add_argument("--groups", type=int, default=2, help="число групп процессов")
    p.add_argument("--seed", type=int, default=42, help="зерно генератора")
    p.add_argument("--dir", default="./shared", help="каталог общих файлов")
    p.add_argument("--make-input", action="store_true", help="только создать входные файлы")
    return p


def _print_summary(world, args, t_pair, t_coll, group_gathered):
    """Печатает сводку сравнения (на процессе 0 мира)."""
    print(f"\n=== ЛР8, n={args.size}, процессов={world.Get_size()}, групп={args.groups} ===")
    print(f"парные операции (ЛР7), весь мир:   {t_pair:.3f} с")
    print(f"коллективные операции, весь мир:   {t_coll:.3f} с")
    seen = {}
    for gid, gtime, grank in group_gathered:
        if grank == 0:
            seen[gid] = gtime
    for gid in sorted(seen):
        print(f"группа {gid}: умножение из файлов = {seen[gid]:.3f} с")


def _ensure_input(world, args):
    """Процесс 0 создаёт входные файлы, если их нет; все ждут."""
    path_a = os.path.join(args.dir, "matrixA.bin")
    if world.Get_rank() == 0 and not os.path.exists(path_a):
        make_input_files(args.dir, args.size, args.seed)
    world.Barrier()


def main() -> int:
    """Точка входа MPI-программы ЛР8."""
    args = build_parser().parse_args()
    if args.make_input:
        make_input_files(args.dir, args.size, args.seed)
        return 0
    world = MPI.COMM_WORLD
    _ensure_input(world, args)
    t_pair = _pairwise_baseline(world, args.size, args.seed)
    _c, t_coll = _timed_collective(world, args)
    colors = plan_groups(world.Get_size(), args.groups, args.seed)
    gid = colors[world.Get_rank()]
    group = world.Split(color=gid, key=world.Get_rank())
    gtime = group_multiply_files(group, gid, args.size, args.dir)
    gathered = world.gather((gid, gtime, group.Get_rank()), root=0)
    if world.Get_rank() == 0:
        _print_summary(world, args, t_pair, t_coll, gathered)
    group.Free()
    return 0


def _timed_collective(world, args):
    """Замеряет коллективное умножение на всём мире."""
    a = lab7.make_matrix(args.size, args.seed) if world.Get_rank() == 0 else None
    b = lab7.make_matrix(args.size, args.seed + 1) if world.Get_rank() == 0 else None
    world.Barrier()
    t0 = MPI.Wtime()
    c = collective_multiply(world, args.size, a, b)
    return c, MPI.Wtime() - t0


if __name__ == "__main__":
    raise SystemExit(main())
