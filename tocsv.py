import pandas as pd

# Đường dẫn tới file TXT
file_path = "./dataset/london_tube_map.txt"

# Đọc file TXT và chuyển thành DataFrame
edges_df = pd.read_csv(file_path, sep="\s+", header=None, names=['src', 'dst'])

# Hiển thị DataFrame
print(edges_df)

# Xuất DataFrame thành file CSV với mã hóa UTF-8
edges_df.to_csv('./dataset/london_tube_map.csv', index=False, header=True, encoding='utf-8')

print("Dữ liệu đã được chuyển đổi và lưu thành CSV thành công với mã hóa UTF-8.")
