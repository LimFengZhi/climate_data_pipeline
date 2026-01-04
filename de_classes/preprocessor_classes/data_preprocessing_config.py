# Author: THAM ZHEN HERN
class DataPreprocessingConfig:
    def __init__(self, primary_key_cols: list[str],
                 remove_cols: list[str] = None,
                 handle_mean_cols: list[str] = None,
                 handle_median_cols: list[str] = None,
                 handle_mode_cols: list[str] = None,
                 handle_value_cols: dict = None,
                 handle_remove_value_cols: list[str]= None, 
                 alpha_cols: list[str] = None,
                 numeric_cols: list[str] = None,
                 double_cols: list[str] = None,
                 integer_cols: list[str] = None):

        self.primary_key_cols = primary_key_cols
        self.remove_cols = remove_cols or []
        self.handle_mean_cols = handle_mean_cols or []
        self.handle_median_cols = handle_median_cols or []
        self.handle_mode_cols = handle_mode_cols or []
        self.handle_value_cols = handle_value_cols or {}
        self.handle_remove_value_cols = handle_remove_value_cols or []
        self.alpha_cols = alpha_cols or []
        self.integer_cols = integer_cols or []
        self.double_cols = double_cols or []