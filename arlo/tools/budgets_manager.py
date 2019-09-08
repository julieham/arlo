from operations.df_operations import concat_lines, filter_df_not_this_value, add_column_with_value
from parameters.column_names import cycle_col, category_col, amount_euro_col, currency_col
from parameters.param import default_currency
from read_write.file_manager import read_budgets, save_budgets


def edit_budgets_cycle(budgets_df):
    all_budgets = concat_lines([read_budgets(), budgets_df])
    all_budgets.drop_duplicates(keep='first', inplace=True)  # discards previously equal budgets
    all_budgets.drop_duplicates(subset=[category_col, cycle_col], keep='last', inplace=True)  # replaces w new values
    add_column_with_value(all_budgets, currency_col, default_currency)
    save_budgets(filter_df_not_this_value(all_budgets, amount_euro_col, 0))
