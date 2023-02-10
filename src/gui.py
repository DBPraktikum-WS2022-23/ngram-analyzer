import os
import tkinter as tk
import tkinter.font as fnt
from typing import List

from controller import SparkController
from config_converter import ConfigConverter
from controller import PluginController
from database_creation import NgramDBBuilder

class CenterFrame(tk.Frame):
    def __init__(self, master, relief, height = None, width = None) -> None:
        super().__init__(master=master, relief=relief, height=height, width=width) 
        self.spark_controller = SparkConnection().spark_controller
        self.__add_plot_output()
        self.__add_console_output()
        self.__add_console_input()

    def __add_console_input(self) -> None:
        self.entry = tk.Entry(self, width=70)
        self.button = tk.Button(self, text="Run", command=self.__execute)
        self.entry.grid(row=2, column=0)
        self.button.grid(row=2, column=1)

    def __add_console_output(self) -> None:
        self.text = tk.Text(self)
        self.text.grid(row=1, column=0)

    def __add_plot_output(self) -> None:
        self.plot = tk.Label(self, text="Placeholder_Plot")
        self.plot.grid(row=0, column=0)

    def __execute(self):
        output = self.spark_controller.execute_sql(self.entry.get())._jdf.showString(100, 100, False)
        self.__print_output(output)

    def __print_output(self, output) -> None:
        self.text.insert('end', output + "\n")
        self.text.config(state='disabled')

    def update_input(self, input: str) -> None:
        self.entry.delete(0, 'end')
        self.entry.insert(0, input)

class FunctionFrame(tk.Frame):
    """Frame on the right side with function to generate queries"""

    def __init__(self, master, relief, bd) -> None:
        super().__init__(master=master, relief=relief, bd=bd)

        # Function 1: Plot word frequencies
        frm_func1 = tk.Frame(self, relief=tk.RAISED, bd=2)
        frm_func1.pack(fill="both", expand=True)

        func1_title = tk.Label(frm_func1, text="Plot word frequencies                                        ")
        func1_title.grid(row=0, column=0, columnspan=3, sticky="w")

        func1_start_label = tk.Label(frm_func1, text="Start")
        func1_start_label.grid(row=1, column=0, sticky="ew")
        func1_start_input = tk.Entry(frm_func1, width=4)
        func1_start_input.grid(row=2, column=0, sticky="ew")

        func1_end_label = tk.Label(frm_func1, text="End")
        func1_end_label.grid(row=1, column=1, sticky="ew")
        func1_end_input = tk.Entry(frm_func1, width=4)
        func1_end_input.grid(row=2, column=1, sticky="ew")

        func1_btn_execute = tk.Button(frm_func1, text="Generate query", font=fnt.Font(size=8), anchor="e")
        func1_btn_execute.grid(row=2, column=2, sticky="e")

        for widget in frm_func1.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_func1.grid(row=0, column=0, sticky="new")

        # Function 2: Highes relative change
        frm_func2 = tk.Frame(self, relief=tk.RAISED, bd=2)

        func2_title = tk.Label(frm_func2, text="Highest relative change")
        func2_title.grid(row=0, column=0, columnspan=3, sticky="w")

        func2_start_label = tk.Label(frm_func2, text="Start")
        func2_start_label.grid(row=1, column=0, sticky="ew")
        func2_start_input = tk.Entry(frm_func2, width=4)
        func2_start_input.grid(row=2, column=0, sticky="ew")

        func2_end_label = tk.Label(frm_func2, text="End")
        func2_end_label.grid(row=1, column=1, sticky="ew")
        func2_end_input = tk.Entry(frm_func2, width=4)
        func2_end_input.grid(row=2, column=1, sticky="ew")

        func2_btn_execute = tk.Button(frm_func2, text="Generate query", font=fnt.Font(size=8))
        func2_btn_execute.config(command=lambda: self.hrc(func2_end_input.get()))  # TODO: duration or start and end?
        func2_btn_execute.grid(row=2, column=2, sticky="e")

        for widget in frm_func2.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_func2.grid(row=1, column=0, sticky="nsew")

        # Function 3: Pearson correlation coefficient
        frm_func3 = tk.Frame(self, relief=tk.RAISED, bd=2)

        func3_title = tk.Label(frm_func3, text="Pearson correlation coefficient")
        func3_title.grid(row=0, column=0, columnspan=3, sticky="w")

        func3_start_label = tk.Label(frm_func3, text="Start")
        func3_start_label.grid(row=1, column=0, sticky="ew")
        func3_start_input = tk.Entry(frm_func3, width=4)
        func3_start_input.grid(row=2, column=0, sticky="ew")

        func3_end_label = tk.Label(frm_func3, text="End")
        func3_end_label.grid(row=1, column=1, sticky="ew")
        func3_end_input = tk.Entry(frm_func3, width=4)
        func3_end_input.grid(row=2, column=1, sticky="ew")

        func3_btn_execute = tk.Button(frm_func3, text="Generate query", font=fnt.Font(size=8))
        func3_btn_execute.grid(row=2, column=2, sticky="e")

        for widget in frm_func3.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_func3.grid(row=2, column=0, sticky="ew")

        # Function 4: Euclidean distance of nearest neighbours
        frm_func4 = tk.Frame(self, relief=tk.RAISED, bd=2)

        func4_title = tk.Label(frm_func4, text="Euclidean distance of nearest neighbours")
        func4_title.grid(row=0, column=0, columnspan=3, sticky="w")

        func4_number_label = tk.Label(frm_func4, text="Number of neighbours")
        func4_number_label.grid(row=1, column=0, columnspan=2, sticky="ew")
        func4_number_input = tk.Entry(frm_func4, width=15)
        func4_number_input.grid(row=2, column=0, columnspan=2, sticky="ew")

        func4_btn_execute = tk.Button(frm_func4, text="Generate query", font=fnt.Font(size=8))
        func4_btn_execute.grid(row=2, column=2, sticky="e")

        for widget in frm_func4.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_func4.grid(row=3, column=0, sticky="ew")

        # Function Template
        frm_funcn = tk.Frame(self, relief=tk.RAISED, bd=2)

        funcn_title = tk.Label(frm_funcn, text="Example Function")
        funcn_title.grid(row=0, column=0, columnspan=3, sticky="w")

        funcn_start_label = tk.Label(frm_funcn, text="Start")
        funcn_start_label.grid(row=1, column=0, sticky="ew")
        funcn_start_input = tk.Entry(frm_funcn, width=4)
        funcn_start_input.grid(row=2, column=0, sticky="ew")

        funcn_end_label = tk.Label(frm_funcn, text="End")
        funcn_end_label.grid(row=1, column=1, sticky="ew")
        funcn_end_input = tk.Entry(frm_funcn, width=4)
        funcn_end_input.grid(row=2, column=1, sticky="ew")

        funcn_btn_execute = tk.Button(frm_funcn, text="Generate query", font=fnt.Font(size=8))
        funcn_btn_execute.grid(row=2, column=2, sticky="e")

        for widget in frm_funcn.winfo_children():
            widget.grid(padx=1, pady=1)

        frm_funcn.grid(row=99, column=0, sticky="nsew")  # TODO: change row!

    def hrc(self, duration: int) -> None:
        query = f"""
        select hrc.str_rep word, hrc.type type, hrc.start_year start, hrc.end_year end, hrc.result hrc
        from (select hrc({duration},*) as hrc from schema_f limit 1)
        """
        # TODO: pass query to CenterFrame


class NgramFrame(tk.Frame):
    """Frame on the left side listing N-grams"""
    def __init__(self, master, relief, bd) -> None:
        super().__init__(master=master, relief=relief, bd=bd)

        self.spark_controller = SparkConnection().spark_controller

        frm_buttons = tk.Frame(self, relief=tk.RAISED, bd=2)
        btn_open = tk.Button(frm_buttons, text="Add Ngram", font=fnt.Font(size=8))
        btn_save = tk.Button(frm_buttons, text="Remove Ngram", font=fnt.Font(size=8))
        btn_deselect = tk.Button(frm_buttons, text="Deselect All", font=fnt.Font(size=8))
        btn_open.grid(row=0, column=0, sticky="ew")
        btn_save.grid(row=0, column=1, sticky="ew")
        btn_deselect.grid(row=0, column=2, sticky="ew")

        frm_buttons.grid(row=0, column=0, sticky="nws")

        for widget in frm_buttons.winfo_children():
            widget.grid(padx=1, pady=5)
        items = ["aaa", "bbb", "ccc"]
        list_items = tk.Variable(value=items)
        self.__listbox = tk.Listbox(self, listvariable=list_items, height=100)
        self.__listbox.grid(row=1, column=0, sticky="ew")#
        self.__listbox.bind('<<ListboxSelect>>', self.items_selected)

        self.selected_str = []

    def items_selected(self, event):
        # get all selected indices
        selected_indices = self.__listbox.curselection()
        # get selected items
        selected_langs = ",".join([self.__listbox.get(i) for i in selected_indices])
        self.selected_str = selected_langs
        print(selected_langs)

    def __update_ngrams(self, ngrams: list):
        self.spark_controller.create_ngram_view(ngrams)


class GUI():
    def __init__(self) -> None:
        self.window = tk.Tk()

        self.window.title("NGram Visualizer")

        self.window.rowconfigure(0, minsize=600, weight=1)
        self.window.columnconfigure([0, 1, 2], minsize=200, weight=1)

        frm_functions = NgramFrame(self.window, relief=tk.RAISED, bd=2)
        frm_functions.grid(row=0, column=0, sticky="nws")

        frm_center = CenterFrame(self.window, relief=tk.FLAT, height=400, width=400)
        frm_center.grid(row=0, column=1)

        frm_functions = FunctionFrame(self.window, relief=tk.RAISED, bd=2)
        frm_functions.grid(row=0, column=2, sticky="nes")

    def show(self):
        self.window.mainloop()


class SparkConnection():
    # TODO: don't need this class when shell is initialized
    def __init__(self) -> None:
        config: ConfigConverter = ConfigConverter(
            "settings/" + os.listdir("settings")[0] # temporary
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