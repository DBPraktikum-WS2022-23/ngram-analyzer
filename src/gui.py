import tkinter as tk
from tkinter import ttk
import tkinter.font as fnt

from typing import Type, List

class GUI(tk.Tk):
    """Wrapper for tkinter root object"""
    def __init__(self) -> None:
        super().__init__()

        self.title("NGram Visualizer")

        self.rowconfigure(0, minsize=200, weight=1)
        self.columnconfigure([0, 1, 2], minsize=200, weight=1)

        self.__word_list = ["word liste aus GUI"]
        self.__selected_word_list = []
        frm_functions = NgramFrame(self, relief=tk.RAISED, bd=2)
        frm_functions.grid(row=0, column=0, sticky="nws")

        frm_center = CenterFrame(self, relief=tk.FLAT, height=400, width=400)
        frm_center.grid(row=0, column=1)

        frm_functions = FunctionFrame(self, relief=tk.RAISED, bd=2)
        frm_functions.grid(row=0, column=2, sticky="nes")

    def set_word_list(self, words) -> None:
        self.__word_list = words
        print(self.__word_list)

    def get_word_list(self) -> List[str]:
        return self.__word_list

    def set_selected_word_list(self, words) -> None:
        self.__selected_word_list = words

    def get_selected_word_list(self) -> List[str]:
        return self.__selected_word_list

    def show(self):
        self.mainloop()

class CenterFrame(tk.Frame):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs) 
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
        self.entry.grid(row=1, column=0, sticky=tk.W+tk.E)
        self.button.grid(row=1, column=1, sticky=tk.W+tk.E)

    def __execute(self):
        print(self.master.get_word_list()) # wird in terminal ausgegeben nicht in GUI
        self.__print_output("Test")

    def __print_output(self, output) -> None:
        self.text.insert('end', output + "\n")
        self.text.config(state='disabled')

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

class NgramFrame(tk.Frame):
    """Frame on the left side listing N-grams"""
    def __init__(self, master, relief, bd) -> None:
        super().__init__(master=master, relief=relief, bd=bd)

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
        self.__listbox.bind('<<ListboxSelect>>', self.__items_selected)

    def __items_selected(self, event):
        # get all selected indices
        selected_indices = self.__listbox.curselection()
        # get selected items
        selected_items = [self.__listbox.get(i) for i in selected_indices]
        self.master.set_selected_word_list(selected_items)
        self.__update_itemlist()

    def __update_itemlist(self):
        self.master.set_word_list([self.__listbox.get(i) for i in range(self.__listbox.size())])


if __name__ == "__main__":
    GUI().show()