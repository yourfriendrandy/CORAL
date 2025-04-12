import sqlite3
import csv

# **Database connection**
def export_motif_parameters_to_csv():
    conn = sqlite3.connect("motif_parameters.db")
    cursor = conn.cursor()
    
    # **Fetch A, B, L, Y from the database**
    cursor.execute("SELECT A, B, L, Y FROM motif_parameters")
    rows = cursor.fetchall()
    conn.close()

    # **Write to CSV**
    with open("A_common_scatter.csv", "w", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(["A", "B", "L", "Y"])  # Headers
        writer.writerows(rows)

    print("✅ Data successfully written to A_common_scatter.csv")

# **Run the export**
if __name__ == "__main__":
    export_motif_parameters_to_csv()
