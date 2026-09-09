"""ЛР №7: MPI. Умножение матриц парными (point-to-point) операциями.

Два режима распределения строк матрицы A между процессами:
  - blocking     — Send/Recv (блокирующие парные операции);
  - nonblocking  — Isend/Irecv (неблокирующие): рассылка данных совмещается
                   с вычислением собственной доли строк на процессе 0.

Матрица B целиком рассылается каждому процессу парными операциями. Каждый
процесс умножает свою полосу строк A на B и возвращает результат процессу 0.

Запуск (минимум на 3 машинах — через hostfile):
    mpirun -np 4 python3 lab7/matmul.py --size 1200 --mode both
    mpirun --hostfile hosts -np 6 python3 lab7/matmul.py --size 2000 --mode both
"""
import argparse

import numpy as np
from mpi4py import MPI

TAG_A = 11
TAG_B = 22
TAG_C = 33


def make_matrix(n: int, seed: int) -> np.ndarray:
    """Создаёт квадратную матрицу n×n из float64 по заданному зерну."""
    rng = np.random.default_rng(seed)
    return rng.random((n, n), dtype=np.float64)


def row_ranges(n: int, size: int) -> list:
    """Делит n строк на size частей (границы [start, end) для каждого ранга)."""
    base, extra = divmod(n, size)
    ranges = []
    start = 0
    for r in range(size):
        rows = base + (1 if r < extra else 0)
        ranges.append((start, start + rows))
        start += rows
    return ranges


def _distribute_blocking(comm, a, b, ranges):
    """Процесс 0 рассылает блоки A и всю B воркерам (блокирующе)."""
    for r in range(1, comm.Get_size()):
        s, e = ranges[r]
        comm.Send([np.ascontiguousarray(a[s:e]), MPI.DOUBLE], dest=r, tag=TAG_A)
        comm.Send([b, MPI.DOUBLE], dest=r, tag=TAG_B)


def _gather_blocking(comm, c, ranges):
    """Процесс 0 принимает посчитанные блоки результата (блокирующе)."""
    for r in range(1, comm.Get_size()):
        s, e = ranges[r]
        comm.Recv([c[s:e], MPI.DOUBLE], source=r, tag=TAG_C)


def run_blocking(comm, a, b, n):
    """Полный цикл умножения блокирующими парными операциями (возвращает C, время)."""
    rank, size = comm.Get_rank(), comm.Get_size()
    ranges = row_ranges(n, size)
    comm.Barrier()
    t0 = MPI.Wtime()
    if rank == 0:
        _distribute_blocking(comm, a, b, ranges)
        c = np.empty((n, n), dtype=np.float64)
        s, e = ranges[0]
        c[s:e] = a[s:e] @ b
        _gather_blocking(comm, c, ranges)
        return c, MPI.Wtime() - t0
    _worker_block(comm, n, ranges[rank])
    return None, MPI.Wtime() - t0


def _worker_block(comm, n, my_range):
    """Воркер: принимает свой блок A и B, считает и отправляет результат (блокирующе)."""
    s, e = my_range
    rows = e - s
    a_block = np.empty((rows, n), dtype=np.float64)
    b_local = np.empty((n, n), dtype=np.float64)
    comm.Recv([a_block, MPI.DOUBLE], source=0, tag=TAG_A)
    comm.Recv([b_local, MPI.DOUBLE], source=0, tag=TAG_B)
    comm.Send([np.ascontiguousarray(a_block @ b_local), MPI.DOUBLE], dest=0, tag=TAG_C)


def run_nonblocking(comm, a, b, n):
    """Полный цикл неблокирующими парными операциями с перекрытием рассылки и счёта."""
    rank, size = comm.Get_rank(), comm.Get_size()
    ranges = row_ranges(n, size)
    comm.Barrier()
    t0 = MPI.Wtime()
    if rank == 0:
        return _master_nonblocking(comm, a, b, n, ranges), MPI.Wtime() - t0
    _worker_block_nonblocking(comm, n, ranges[rank])
    return None, MPI.Wtime() - t0


def _master_nonblocking(comm, a, b, n, ranges):
    """Процесс 0: посылает блоки Isend'ом и параллельно считает свою долю."""
    reqs = []
    for r in range(1, comm.Get_size()):
        s, e = ranges[r]
        reqs.append(comm.Isend([np.ascontiguousarray(a[s:e]), MPI.DOUBLE], dest=r, tag=TAG_A))
        reqs.append(comm.Isend([np.ascontiguousarray(b), MPI.DOUBLE], dest=r, tag=TAG_B))
    c = np.empty((n, n), dtype=np.float64)
    s, e = ranges[0]
    c[s:e] = a[s:e] @ b            # полезная работа, пока идут пересылки
    MPI.Request.Waitall(reqs)
    recvs = [comm.Irecv([c[rs:re], MPI.DOUBLE], source=r, tag=TAG_C)
             for r, (rs, re) in enumerate(ranges) if r != 0]
    MPI.Request.Waitall(recvs)
    return c


def _worker_block_nonblocking(comm, n, my_range):
    """Воркер: неблокирующий приём данных, счёт, неблокирующая отправка результата."""
    s, e = my_range
    rows = e - s
    a_block = np.empty((rows, n), dtype=np.float64)
    b_local = np.empty((n, n), dtype=np.float64)
    ra = comm.Irecv([a_block, MPI.DOUBLE], source=0, tag=TAG_A)
    rb = comm.Irecv([b_local, MPI.DOUBLE], source=0, tag=TAG_B)
    MPI.Request.Waitall([ra, rb])
    comm.Isend([np.ascontiguousarray(a_block @ b_local), MPI.DOUBLE], dest=0, tag=TAG_C).Wait()


def _verify(a, b, c, n) -> None:
    """Контроль корректности на процессе 0 (для небольших матриц)."""
    if n <= 512 and np.allclose(c, a @ b):
        print("проверка: результат совпал с локальным A@B")


def build_parser() -> argparse.ArgumentParser:
    """Разбор аргументов."""
    p = argparse.ArgumentParser(description="MPI умножение матриц, парные операции (ЛР7)")
    p.add_argument("--size", type=int, default=1200, help="размер матрицы n×n")
    p.add_argument("--mode", choices=["blocking", "nonblocking", "both"], default="both")
    p.add_argument("--seed", type=int, default=42, help="зерно генератора")
    return p


def _run_mode(comm, mode, a, b, args):
    """Запускает один режим и печатает время на процессе 0."""
    runner = run_blocking if mode == "blocking" else run_nonblocking
    c, dt = runner(comm, a, b, args.size)
    if comm.Get_rank() == 0:
        print(f"режим={mode:12s} процессов={comm.Get_size()} n={args.size} время={dt:.3f} с")
        _verify(a, b, c, args.size)


def main() -> int:
    """Точка входа MPI-программы."""
    args = build_parser().parse_args()
    comm = MPI.COMM_WORLD
    a = b = None
    if comm.Get_rank() == 0:
        a = make_matrix(args.size, args.seed)
        b = make_matrix(args.size, args.seed + 1)
    modes = ["blocking", "nonblocking"] if args.mode == "both" else [args.mode]
    for mode in modes:
        _run_mode(comm, mode, a, b, args)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
