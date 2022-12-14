from cmd import Cmd
# TO DO: über pfeiltasten vorherigen befehl holen

class Prompt(Cmd):
    intro: str = ('Welcome to the ngram_analyzer shell. Type help or ? to list commands.\n')
    prompt: str = '(ngram_analyzer) '

    def do_exit(self, inp):
        return True


Prompt().cmdloop()