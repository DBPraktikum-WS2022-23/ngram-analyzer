import tkinter as tk
import tkinter.font as fnt

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

    def show(self):
        self.window.mainloop()


if __name__ == "__main__":
    GUI().show()