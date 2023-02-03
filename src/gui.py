import tkinter as tk
import tkinter.font as fnt

class CenterFrame(tk.Frame):
    def __init__(self, master, relief) -> None:
        super().__init__(master=master, relief=relief)
        
        plot_output = tk.Label(self, text="Placeholder_Plot")
        console_output = tk.Label(self, text="Placeholder_Console_Output")
        sql_input = tk.Entry(self)
        sql_button = tk.Button(self, text="Run")
        plot_output.grid(row=0, column=0)
        console_output.grid(row=1, column=0)
        sql_input.grid(row=2, column=0)
        sql_button.grid(row=2, column=1)

    # TODO: link button fct
    # TODO: replace placeholder in labels


class GUI():
    def __init__(self) -> None:
        self.window = tk.Tk()

        self.window.title("NGram Visualizer")

        self.window.rowconfigure(0, minsize=600, weight=1)
        self.window.columnconfigure([0, 1, 2], minsize=200, weight=1)

        frm_buttons = tk.Frame(self.window, relief=tk.RAISED, bd=2)

        btn_open = tk.Button(frm_buttons, text="Add Ngram", font = fnt.Font(size = 8))

        btn_save = tk.Button(frm_buttons, text="Remove Ngram", font = fnt.Font(size = 8))

        btn_deselect = tk.Button(frm_buttons, text="Deselect All", font = fnt.Font(size = 8))

        btn_open.grid(row=0, column=0, sticky="ew")

        btn_save.grid(row=0, column=1, sticky="ew")

        btn_deselect.grid(row=0, column=2, sticky="ew")

        frm_buttons.grid(row=0, column=0, sticky="ns")

        for widget in frm_buttons.winfo_children():
            widget.grid(padx=3, pady=5)

        frm_center = CenterFrame(self.window, relief=tk.FLAT)
        frm_center.grid(row=0, column=1)

    def show(self):
        self.window.mainloop()



if __name__ == "__main__":
    GUI().show()