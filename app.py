from analyzer.analyzer import Analyzer

if __name__ == '__main__':
    print("Please check, if a .env file exists and the variables mentioned in the README are stored within.")
    analyzer = Analyzer()
    analyzer.find_similarities()