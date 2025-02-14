import sys
from transform_bronze_to_silver import transform_to_silver

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("❌ Uso correto: python breweries_silver.py <caminho_para_yaml>")
        sys.exit(1)

    yaml_path = sys.argv[1]
    print(f"📂 Usando arquivo de configuração: {yaml_path}")
    
    transform_to_silver(yaml_path)

