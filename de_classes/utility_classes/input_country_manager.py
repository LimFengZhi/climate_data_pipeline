#author LIMFENGZHI
class InputCountryManager:
    @staticmethod
    def get_country(available_countries):
        print("\nAvailable countries:")
        print(" 0 - All countries")
        for idx, c in enumerate(available_countries, start=1):
            print(f" {idx} - {c}")
        
        choice = input("\nEnter your choice (0 for all): ").strip()
        
        if choice == "0":
            country = None
            return country
        else:
            try:
                idx = int(choice)
                country = available_countries[idx - 1]
                return country
            except (ValueError, IndexError):
                print("Invalid choice. Defaulting to all countries.")
                country = None
                return country