import os
import tkinter as tk
from tkinter import ttk
import tkinter.font as fnt
from tkinter.messagebox import askyesno
from typing import List
from controller import SparkController
from config_converter import ConfigConverter
from controller import PluginController
from database_creation import NgramDBBuilder
from pyspark.sql.functions import col

from typing import Type, List


class GUI(tk.Tk):
    """Wrapper for tkinter root object"""

    def __init__(self) -> None:
        super().__init__()

        self.spark_controller: SparkController = SparkConnection().spark_controller

        self.title("NGram Visualizer")

        self.rowconfigure(0, minsize=200, weight=1)
        self.columnconfigure([0, 1, 2], minsize=200, weight=1)

        self.__word_list = ["word liste aus GUI"]
        self.__selected_word_list = []
        frm_functions = NgramFrame(self, relief=tk.RAISED, bd=2)
        frm_functions.grid(row=0, column=0, sticky="nws")

        frm_center = CenterFrame(self, relief=tk.FLAT, height=400, width=400)
        frm_center.grid(row=0, column=1)

        frm_functions = FunctionFrame(self, relief=tk.RAISED, bd=2, center_frame=frm_center)
        frm_functions.grid(row=0, column=2, sticky="nes")

    def set_word_list(self, words) -> None:
        self.__word_list = words

    def get_word_list(self) -> List[str]:
        return self.__word_list

    def set_selected_word_list(self, words) -> None:
        self.__selected_word_list = words

    def get_selected_word_list(self) -> List[str]:
        return self.__selected_word_list

    def get_spark_controller(self) -> SparkController:
        return self.spark_controller

    def show(self):
        self.mainloop()


class CenterFrame(tk.Frame):
    def __init__(self, master, relief, height, width) -> None:
        super().__init__(master=master, relief=relief, height=height, width=width)
        self.__spark_ctrl = master.get_spark_controller()  # master is the GUI object
        self.__add_plot_output()
        self.__add_tabs_notebook()

    def __add_plot_output(self) -> None:
        self.plot = tk.Label(self, text="Placeholder_Plot")
        self.plot.grid(row=0, column=0, columnspan=2)

    def __add_tabs_notebook(self) -> None:
        self.notebook = ttk.Notebook(self)

        self.console_tab = ttk.Frame(self.notebook)
        self.sql_tab = ttk.Frame(self.notebook)

        self.notebook.add(self.console_tab, text="Console")
        self.notebook.add(self.sql_tab, text="SQL")
        self.notebook.grid(row=1, column=0)

        self.__add_sql_output(self.sql_tab)
        self.__add_sql_input(self.sql_tab)

        self.__add_console(self.console_tab)

    def __add_console(self, master) -> None:
        self.console = tk.Label(master, text="Placeholder_Console")
        self.console.grid(row=0, column=0)

    def __add_sql_output(self, master) -> None:
        self.text = tk.Text(master)
        self.text.grid(row=0, column=0, columnspan=2, rowspan=1)

    def __add_sql_input(self, master) -> None:
        self.entry = tk.Entry(master, width=70)
        self.button = tk.Button(master, text="Run", command=self.__execute, font=fnt.Font(size=8))
        self.entry.grid(row=1, column=0, sticky=tk.W + tk.E)
        self.button.grid(row=1, column=1, sticky=tk.W + tk.E)

    def __execute(self):
        self.__spark_ctrl.create_ngram_view(self.master.get_word_list())
        output = self.__spark_ctrl.execute_sql(self.entry.get())._jdf.showString(100, 100, False)
        self.__print_output(output)

    def __print_output(self, output) -> None:
        self.text.insert('end', output + "\n")
        self.text.config(state='disabled')

    def update_input(self, input: str) -> None:
        self.entry.delete(0, 'end')
        self.entry.insert(0, input)


class FunctionFrame(tk.Frame):
    """Frame on the right side with function to generate queries"""

    def __init__(self, master, relief, bd, center_frame: CenterFrame) -> None:
        center_frame = center_frame
        word_list = ["Fehlerlos", "Fallschirmzubehör", "Dokumentation"]
        spark_ctrl = master.get_spark_controller()
        super().__init__(master=master, relief=relief, bd=bd)

        # Function 1: Highest Relative Change
        @staticmethod
        def gen_query_f1():
            dur = f1_dur_input.get()
            word_list_str = ", ".join("'" + word + "'" for word in word_list)
            query = f"select hrc.str_rep word, hrc.type type, hrc.start_year start, hrc.end_year end, hrc.result hrc from (select hrc({dur}, *) hrc from schema_f where str_rep in ({word_list_str}))"
            test_output.config(text=query)
            center_frame.update_input(query)

        frm_f1 = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_f1.columnconfigure(0, weight=0)
        frm_f1.columnconfigure(1, weight=1)
        frm_f1.columnconfigure(2, weight=0)

        f1_title = tk.Label(frm_f1, text="Highest Relative Change")
        f1_title.grid(row=0, column=0, columnspan=3, sticky="w")

        f1_dur_label = tk.Label(frm_f1, text="Duration")
        f1_dur_label.grid(row=1, column=0, sticky="ew")
        f1_dur_input = tk.Entry(frm_f1, width=8)
        f1_dur_input.grid(row=2, column=0, sticky="ew")

        f1_btn_execute = tk.Button(frm_f1, text="Generate query", font=fnt.Font(size=8), command=gen_query_f1,
                                   anchor="e")
        f1_btn_execute.grid(row=2, column=2, sticky="e")

        for widget in frm_f1.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_f1.grid(row=1, column=0, sticky="new")

        # Function 2: Pearson correlation coefficient
        def gen_query_f2():
            start = f2_start_input.get()
            end = f2_end_input.get()
            word_list_str = ", ".join("'" + word + "'" for word in word_list)
            query = f"with sel_words as (select * from schema_f where str_rep in ({word_list_str})) select pc.str_rep_1 word_1, pc.type_1 type_1, pc.str_rep_2 word_2, pc.type_2 type_2, pc.start_year start, pc.end_year end, pc.result pearson_corr from (select pc({start}, {end}, *) pc from sel_words a cross join sel_words b where a.str_rep != b.str_rep)"
            test_output.config(text=query)
            center_frame.update_input(query)

        frm_f2 = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_f2.columnconfigure(0, weight=0)
        frm_f2.columnconfigure(1, weight=0)
        frm_f2.columnconfigure(2, weight=1)
        frm_f2.columnconfigure(3, weight=0)

        f2_title = tk.Label(frm_f2, text="Pearson correlation coefficient")
        f2_title.grid(row=0, column=0, columnspan=3, sticky="w")

        f2_start_label = tk.Label(frm_f2, text="Start")
        f2_start_label.grid(row=1, column=0, sticky="ew")
        f2_start_input = tk.Entry(frm_f2, width=6)
        f2_start_input.grid(row=2, column=0, sticky="ew")
        f2_end_label = tk.Label(frm_f2, text="End")
        f2_end_label.grid(row=1, column=1, sticky="ew")
        f2_end_input = tk.Entry(frm_f2, width=6)
        f2_end_input.grid(row=2, column=1, sticky="ew")

        f2_btn_execute = tk.Button(frm_f2, text="Generate query", font=fnt.Font(size=8), command=gen_query_f2)
        f2_btn_execute.grid(row=2, column=3, sticky="e")

        for widget in frm_f2.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_f2.grid(row=2, column=0, sticky="nsew")

        # Function 3: Statistical features for time series
        def gen_query_f3():
            word_list_str = ", ".join("'" + word + "'" for word in word_list)
            query = f"with sel_words as (select * from schema_f where str_rep in ({word_list_str})) select sf.str_rep, sf.type, sf.mean, sf.median, sf.q_25, sf.q_75, sf.var, sf.min, sf.max, sf.hrc from (select sf(*) sf from sel_words)"
            test_output.config(text=query)
            center_frame.update_input(query)

        frm_f3 = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_f3.columnconfigure(0, weight=1)
        frm_f3.columnconfigure(1, weight=1)
        frm_f3.columnconfigure(2, weight=0)

        f3_title = tk.Label(frm_f3, text="Statistical features for time series")
        f3_title.grid(row=0, column=0, columnspan=3, sticky="w")

        f3_btn_execute = tk.Button(frm_f3, text="Generate query", font=fnt.Font(size=8), command=gen_query_f3)
        f3_btn_execute.grid(row=1, column=2, sticky="e")

        for widget in frm_f3.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_f3.grid(row=3, column=0, sticky="nsew")

        # Function 4: Relations between pairs of time series
        def gen_query_f4():
            word_list_str = ", ".join("'" + word + "'" for word in word_list)
            query = f"with sel_words as (select * from sel_words where str_rep in ({word_list_str})) select rel.str_rep1, rel.type1, rel.str_rep2, rel.type2, rel.hrc_year, rel.hrc_max, rel.cov, rel.spearman_corr, rel.pearson_corr from (select rel(*) rel from sel_words a cross join sel_words b where a.str_rep != b.str_rep)"
            test_output.config(text=query)
            center_frame.update_input(query)

        frm_f4 = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_f4.columnconfigure(0, weight=1)
        frm_f4.columnconfigure(1, weight=1)
        frm_f4.columnconfigure(2, weight=0)

        f4_title = tk.Label(frm_f4, text="Relations between pairs of time series")
        f4_title.grid(row=0, column=0, columnspan=3, sticky="w")

        f4_btn_execute = tk.Button(frm_f4, text="Generate query", font=fnt.Font(size=8),
                                   command=gen_query_f4)
        f4_btn_execute.grid(row=1, column=2, sticky="e")

        for widget in frm_f4.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_f4.grid(row=4, column=0, sticky="nsew")

        # Function 5: Linear regression
        def gen_query_f5():
            word_list_str = ", ".join("'" + word + "'" for word in word_list)
            query = f"select lr.type type, lr.slope slope, lr.intercept intercept, lr.r_value r_value, lr.p_value p_value, lr.std_err std_err from (select lr(*) lr from schema_f where str_rep in ({word_list_str}))"
            test_output.config(text=query)
            center_frame.update_input(query)

        frm_f5 = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_f5.columnconfigure(0, weight=1)
        frm_f5.columnconfigure(1, weight=1)
        frm_f5.columnconfigure(2, weight=0)

        f5_title = tk.Label(frm_f5, text="Linear regression")
        f5_title.grid(row=0, column=0, columnspan=3, sticky="w")

        f5_btn_execute = tk.Button(frm_f5, text="Generate query", font=fnt.Font(size=8), command=gen_query_f5)
        f5_btn_execute.grid(row=1, column=2, sticky="e")

        for widget in frm_f5.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_f5.grid(row=5, column=0, sticky="nsew")

        # Function 6: Local outlier factor
        def gen_query_f6():
            k = f6_k_input.get()
            delta = f6_delta_input.get()
            spark_ctrl.create_join_view(word_list)
            # word_subqueries = " cross join ".join("(select * from schema_f where str_rep = '" + word + "')" for word in word_list) + ")"
            query = f"select lof.outlier from (select lof({k},{delta},*) lof from joins)"
            test_output.config(text=query)
            center_frame.update_input(query)

        frm_f6 = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_f6.columnconfigure(0, weight=0)
        frm_f6.columnconfigure(1, weight=0)
        frm_f6.columnconfigure(2, weight=1)
        frm_f6.columnconfigure(3, weight=0)

        f6_title = tk.Label(frm_f6, text="Local outlier factor")
        f6_title.grid(row=0, column=0, columnspan=3, sticky="w")

        f6_k_label = tk.Label(frm_f6, text="k")
        f6_k_label.grid(row=1, column=0, sticky="ew")
        f6_k_input = tk.Entry(frm_f6, width=6)
        f6_k_input.grid(row=2, column=0, sticky="ew")
        f6_delta_label = tk.Label(frm_f6, text="Delta")
        f6_delta_label.grid(row=1, column=1, sticky="ew")
        f6_delta_input = tk.Entry(frm_f6, width=6)
        f6_delta_input.grid(row=2, column=1, sticky="ew")

        f6_btn_execute = tk.Button(frm_f6, text="Generate query", font=fnt.Font(size=8), command=gen_query_f6)
        f6_btn_execute.grid(row=2, column=3, sticky="e")

        for widget in frm_f6.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_f6.grid(row=6, column=0, sticky="nsew")

        # Function 7: K nearest neighbours (euclidean disctance)
        def gen_query_f7():
            k_neighbours = f7_k_neighbours_input.get()
            word = word_list[0]
            query = f"select ed.str_rep, ed.result from (select euclidean_dist(*) ed from schema_f a cross join schema_f b where a.str_rep = '{word}' and b.str_rep != '{word}') order by 2 limit {k_neighbours}"
            test_output.config(text=query)
            center_frame.update_input(query)

        frm_f7 = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_f7.columnconfigure(0, weight=0)
        frm_f7.columnconfigure(1, weight=1)
        frm_f7.columnconfigure(2, weight=0)

        f7_title = tk.Label(frm_f7, text="K nearest neighbours (euclidean disctance)")
        f7_title.grid(row=0, column=0, columnspan=3, sticky="w")

        f7_k_neighbours_label = tk.Label(frm_f7, text="Number of neighbours")
        f7_k_neighbours_label.grid(row=1, column=0, sticky="ew")
        f7_k_neighbours_input = tk.Entry(frm_f7)
        f7_k_neighbours_input.grid(row=2, column=0, sticky="ew")

        f7_btn_execute = tk.Button(frm_f7, text="Generate query", font=fnt.Font(size=8), command=gen_query_f7)
        f7_btn_execute.grid(row=2, column=2, sticky="e")

        for widget in frm_f7.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_f7.grid(row=7, column=0, sticky="nsew")

        # Function 8: Median distance
        def gen_query_f8():
            word_list_str = ", ".join("'" + word + "'" for word in word_list)
            threshold = f8_threshold_input.get()
            query = f"with sel_words as (select * from sel_words where str_rep in ({word_list_str})) select median_distance({threshold}, *) median_distance from sel_words"
            test_output.config(text=query)
            center_frame.update_input(query)

        frm_f8 = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_f8.columnconfigure(0, weight=0)
        frm_f8.columnconfigure(1, weight=1)
        frm_f8.columnconfigure(2, weight=0)

        f8_title = tk.Label(frm_f8, text="Median distance")
        f8_title.grid(row=0, column=0, columnspan=3, sticky="w")

        f8_threshold_label = tk.Label(frm_f8, text="Threshold")
        f8_threshold_label.grid(row=1, column=0, sticky="ew")
        f8_threshold_input = tk.Entry(frm_f8, width=8)
        f8_threshold_input.grid(row=2, column=0, sticky="ew")

        f8_btn_execute = tk.Button(frm_f8, text="Generate query", font=fnt.Font(size=8), command=gen_query_f8)
        f8_btn_execute.grid(row=2, column=2, sticky="e")

        for widget in frm_f8.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_f8.grid(row=8, column=0, sticky="nsew")

        # Function 9: Zscore
        def gen_query_f9():
            word_list_str = ", ".join("'" + word + "'" for word in word_list)
            threshold = f9_threshold_input.get()
            query = f"with sel_words as (select * from sel_words where str_rep in ({word_list_str})) select zscore({threshold}, *) zscore from sel_words"
            test_output.config(text=query)
            center_frame.update_input(query)

        frm_f9 = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_f9.columnconfigure(0, weight=0)
        frm_f9.columnconfigure(1, weight=1)
        frm_f9.columnconfigure(2, weight=0)

        f9_title = tk.Label(frm_f9, text="Zscore")
        f9_title.grid(row=0, column=0, columnspan=3, sticky="w")

        f9_threshold_label = tk.Label(frm_f9, text="Threshold")
        f9_threshold_label.grid(row=1, column=0, sticky="ew")
        f9_threshold_input = tk.Entry(frm_f9, width=8)
        f9_threshold_input.grid(row=2, column=0, sticky="ew")

        f9_btn_execute = tk.Button(frm_f9, text="Generate query", font=fnt.Font(size=8), command=gen_query_f9)
        f9_btn_execute.grid(row=2, column=2, sticky="e")

        for widget in frm_f9.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_f9.grid(row=9, column=0, sticky="nsew")

        # Function Template
        def gen_query_funcn():
            start = funcn_start_input.get()
            end = funcn_end_input.get()
            query = f"select..."
            test_output.config(text=query)
            center_frame.update_input(query)

        frm_funcn = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_funcn.columnconfigure(0, weight=1)
        frm_funcn.columnconfigure(1, weight=1)
        frm_funcn.columnconfigure(2, weight=0)

        funcn_title = tk.Label(frm_funcn, text="Example Function")
        funcn_title.grid(row=0, column=0, columnspan=3, sticky="w")

        funcn_start_label = tk.Label(frm_funcn, text="Start")
        funcn_start_label.grid(row=1, column=0, sticky="ew")
        funcn_start_input = tk.Entry(frm_funcn, width=5)
        funcn_start_input.grid(row=2, column=0, sticky="ew")

        funcn_end_label = tk.Label(frm_funcn, text="End")
        funcn_end_label.grid(row=1, column=1, sticky="ew")
        funcn_end_input = tk.Entry(frm_funcn, width=5)
        funcn_end_input.grid(row=2, column=1, sticky="ew")

        funcn_btn_execute = tk.Button(frm_funcn, text="Generate query", font=fnt.Font(size=8), command=gen_query_funcn)
        funcn_btn_execute.grid(row=2, column=2, sticky="e")

        for widget in frm_funcn.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_funcn.grid(row=99, column=0, sticky="nsew")  # TODO change row

        frm_test = tk.Frame(self, relief=tk.RAISED, bd=2)

        test_output = tk.Label(frm_test, text="Some output", wraplength=400, justify="left")
        # test_output = tk.Text(frm_test, state="disabled")
        # test_output.insert(0.0, "Some output")
        test_output.grid(row=0, column=0, padx=1, pady=1, sticky="nsew")
        frm_test.grid(row=999, column=0, sticky="nsew")


class NgramFrame(tk.Frame):
    """Frame on the left side listing N-grams"""

    def __init__(self, master, relief, bd) -> None:
        super().__init__(master=master, relief=relief, bd=bd)

        self.spark_controller = SparkConnection().spark_controller

        self.__word_df = self.spark_controller.get_word_df()

        self.__selected_items = []
        self.__selected_indices = []

        # initialize buttons
        frm_buttons = tk.Frame(self, relief=tk.RAISED, bd=2)
        self.btn_add = tk.Button(frm_buttons, text="Add Ngram", font=fnt.Font(size=6),
                                 height=1, width=6, command=self.__add_clicked)
        self.btn_remove = tk.Button(frm_buttons, text="Remove Ngram", font=fnt.Font(size=6),
                                    height=1, width=9, command=self.__remove_clicked)
        self.btn_clear = tk.Button(frm_buttons, text="Clear All", font=fnt.Font(size=6),
                                   height=1, width=4, command=self.__clear_clicked)
        self.btn_deselect = tk.Button(frm_buttons, text="Deselect All", font=fnt.Font(size=6),
                                      height=1, width=5, command=self.__deselect_clicked)
        self.btn_add.grid(row=0, column=0, sticky="ew")
        self.btn_remove.grid(row=0, column=1, sticky="ew")
        self.btn_clear.grid(row=0, column=2, sticky="ew")
        self.btn_deselect.grid(row=0, column=3, sticky="ew")
        frm_buttons.grid(row=0, column=0, sticky="nws")

        for widget in frm_buttons.winfo_children():
            widget.grid(padx=0, pady=1)

        # initialize listbox
        items = []

        if self.__word_df.count() < 20:
            items = self.__word_df.select("str_rep").collect()
        else:
            items = self.__word_df.limit(20).select("str_rep").collect()
        items = [row[0] for row in items]
        list_items = tk.Variable(value=items)

        self.__listbox = tk.Listbox(self, listvariable=list_items, height=100, selectmode="multiple")
        self.__listbox.grid(row=1, column=0, sticky="ew")
        self.__listbox.bind('<<ListboxSelect>>', self.__update_selected_items)

    def __add_clicked(self):
        self.win = AddNgramWindow(self.master, self.__insert_item, self.__check_exists, self.__check_duplicate)
        self.master.wait_window(self.win.top)
        self.__update_wordlist()

    def __check_exists(self, item) -> bool:
        return bool(self.__word_df.filter(col("str_rep").contains(item)).collect())

    def __check_duplicate(self, item) -> bool:
        return item in self.__listbox.get(0, tk.END)

    def __insert_item(self, item):
        self.__listbox.insert(tk.END, item)

    def __update_selected_items(self, event):
        self.__selected_indices = self.__listbox.curselection()
        self.__selected_items = [self.__listbox.get(i) for i in self.__selected_indices]
        self.master.set_selected_word_list(self.__selected_items)

    def __remove_clicked(self):
        if self.__selected_items is []:
            return
        if askyesno(title="Remove Ngram",
                    message="Remove selected Ngrams?") is True:
            rev_list = list(self.__selected_indices)
            rev_list.reverse()
            for i in rev_list:
                self.__listbox.delete(i)
            self.__update_wordlist()
        else:
            return

    def __clear_clicked(self):
        if askyesno(title="Clear Ngram List",
                    message="Clear the whole list?") is True:
            self.__listbox.delete(0, tk.END)
            self.__update_wordlist()
        else:
            return

    def __deselect_clicked(self):
        self.__listbox.selection_clear(0, tk.END)
        self.__update_selected_items(None)

    def __update_wordlist(self):
        self.master.set_word_list([self.__listbox.get(i) for i in range(self.__listbox.size())])

    def update_ngrams(self, ngrams: list):
        self.spark_controller.create_ngram_view(ngrams)


class AddNgramWindow(object):
    def __init__(self, master, insert_func, check_exist_func, check_dup_func):
        self.top = tk.Toplevel(master)
        self.insert_func = insert_func
        self.check_exist_func = check_exist_func
        self.check_dup_func = check_dup_func
        self.top.grab_set()
        self.label = tk.Label(self.top, text="Add Ngrams")
        self.label.pack()
        self.entry = tk.Entry(self.top)
        self.entry.pack()
        self.button = tk.Button(self.top, text='Ok', command=self.__cleanup)
        self.button.pack()

    def __cleanup(self):
        new_item = self.entry.get()
        if self.check_dup_func(new_item):
            tk.messagebox.showerror(title="Already exists!", message="Item already exists!")
            return
        if self.check_exist_func(new_item):
            self.insert_func(new_item)
        else:
            tk.messagebox.showerror(title="Not found!", message="Item is not in the database.")
            return
        self.top.destroy()


class SparkConnection():
    # TODO: don't need this class when shell is initialized
    def __init__(self) -> None:
        config: ConfigConverter = ConfigConverter(
            "settings/" + os.listdir("settings")[0]  # temporary
        )
        conn_settings = config.get_conn_settings()
        db_builder = NgramDBBuilder(conn_settings)
        self.spark_controller: SparkController = SparkController(
            conn_settings, log_level="OFF"
        )

        self.plugin_controller: PluginController = PluginController(self.spark_controller.get_spark_session())
        self.plugin_controller.register_plugins()


if __name__ == "__main__":
    GUI().show()
