import tkinter as tk
import tkinter.font as fnt

class CenterFrame(tk.Frame):
    def __init__(self, master, relief, height = None, width = None) -> None:
        super().__init__(master=master, relief=relief, height=height, width=width)
        
        plot_output = self.PlotOutput(self)
        console_output = self.ConsoleOutput(self)
        console_output.print_output('tessst')

        sql_input = self.ConsoleInput(self)

    class ConsoleInput:
        def __init__(self, master) -> None:
            self.master = master
            self.entry = tk.Entry(self.master, width=70)
            self.button = tk.Button(self.master, text="Run")
            self.entry.grid(row=2, column=0)
            self.button.grid(row=2, column=1)

    class ConsoleOutput:
        def __init__(self, master) -> None:
            self.master = master
            self.text = tk.Text(self.master)
            self.text.grid(row=1, column=0)

        def print_output(self, output):
            self.text.insert('end', output + "\n")
            self.text.config(state='disabled')

    class PlotOutput:
        def __init__(self, master) -> None:
            self.master = master
            self.plot = tk.Label(self.master, text="Placeholder_Plot")
            self.plot.grid(row=0, column=0)


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

        frm_buttons.grid(row=0, column=0, sticky="nws")

        for widget in frm_buttons.winfo_children():
            widget.grid(padx=3, pady=5)

        frm_center = CenterFrame(self.window, relief=tk.FLAT, height = 400, width= 400)
        frm_center.grid(row=0, column=1)

    def show(self):
        self.window.mainloop()



if __name__ == "__main__":
    GUI().show()