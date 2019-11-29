def get_pulse_id_mod(n_bytes):
    if n_bytes <= 8:
        return 26214400

    if 8 < n_bytes <= 256:
        return 2621440

    return 25600
