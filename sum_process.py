# Conversion factors
KB_TO_GB = 0.000001
MB_TO_GB = 0.001

values_gb_old = [
    76,23   * KB_TO_GB,  # KB to GB
    16.09,  # GB
    1070.00 * MB_TO_GB,  # MB to GB
    19.65,  # GB
    13.59,  # GB
    17.12,  # GB
    45.52,  # GB
    40.05,  # GB
    46.46,  # GB
     1.85,  # GB
    64.86,  # GB
    20.34,  # GB
    218.30  * MB_TO_GB,  # MB to GB
    9.32    # GB
]

values_gb_new = [
    76,22   * KB_TO_GB,  # KB to GB
     4.80,  # GB
    637.54  * MB_TO_GB,  # MB to GB
     6.93,  # GB
     5.42,  # GB
     5.40,  # GB
    14.14,  # GB
    12.04,  # GB
    14.69,  # GB
     1.37,  # GB
    20.49,  # GB
     6.15,  # GB
    262.40  * MB_TO_GB,  # MB to GB
    2.84    # GB
]

# Sum of all values in GB
print(f" old: {sum(values_gb_old)} \n new: {sum(values_gb_new)} \n diff:{sum(values_gb_old) - sum(values_gb_new)}")