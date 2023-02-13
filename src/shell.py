""" Module for hosting the shell """
import os
from cmd import Cmd
from typing import List

from config_converter import ConfigConverter
from controller import SparkController, PluginController
from database_creation import NgramDBBuilder


class Prompt(Cmd):
    __config_dir = "settings"

    intro: str = (
        "Welcome to the ngram_analyzer shell. Type help or ? to list commands.\n"
    )
    prompt: str = "(ngram_analyzer) "

    def preloop(self) -> None:
        """Get database connection information from user and use it
        to set up the current sessions' configuration."""

        # let user choose configuration file
        print(
            "To use this shell, you need to connect to an already existing database.\n"
            "If you don't have a database yet, you can create one via the CLI "
            "using the db_connect command.\n"
            "If you already have a database, please select the correct config file:\n"
        )
        for idx, file in enumerate(os.listdir(self.__config_dir)):
            print(f"[{idx}] {file}")
        choice: int = int(input("Enter number of config file: "))
        if choice < 0 or choice > len(os.listdir(self.__config_dir)):
            print("Invalid input. Please restart the shell and try again.")
            return

        # read in configuration data
        config: ConfigConverter = ConfigConverter(
            self.__config_dir + "/" + os.listdir(self.__config_dir)[choice]
        )
        # TODO: check if db exists here
        conn_settings = config.get_conn_settings()

        db_builder = NgramDBBuilder(conn_settings)
        if not db_builder.exists_db():
            print("Invalid input. DB does not exist. Please use --db_create and restart shell.")
            return

        print("Successfully connected to database.")
        self.spark_controller: SparkController = SparkController(
            conn_settings, log_level="OFF"
        )
        load_available = input("Would you like to load the plugins available in the src/plugins directory? (y/n) ")

        self.plugin_controller: PluginController = PluginController(self.spark_controller.get_spark_session())
        if load_available in ["n", "no"]:
            self.plugin_controller.register_plugins(plugins_path=input("Please enter path to plugins:"))
        elif load_available in ["y", "yes"]:
            self.plugin_controller.register_plugins()
        else:
            print("Invalid input. Please restart the shell and try again.")
            return

        print("You can now use the shell.")

    def do_print_word_frequencies(self, arg) -> None:
        """Print the frequency of selected words for selected years."""

        words: List[str] = input("Enter words: (separate by space) ").split(" ")
        years: List[str] = input("Enter years: (separate by space) ").split(" ")
        for year in years:
            if not year.isdigit():
                print("Year must be a number.")
                return

        if self.spark_controller is not None:
            self.spark_controller.print_word_frequencies(words, [int(x) for x in years])

    def do_plot_word_frequencies(self, arg) -> None:
        """Plot frequency of words in different years."""

        words: List[str] = input("Enter words: (separate by space) ").split(" ")
        years: List[str] = input("Enter years: (separate by space) ").split(" ")
        for year in years:
            if not year.isdigit():
                print("Year must be a number.")
                return

        if self.spark_controller is not None:
            self.spark_controller.plot_word_frequencies(words, [int(x) for x in years])

    def do_print_db_statistics(self, arg) -> None:
        """Print statistics of the database tables."""
        if self.spark_controller is not None:
            self.spark_controller.print_db_statistics()

    def do_sql(self, arg):
        """Open a prompt to execute SQL queries."""
        print("Welcome to the SQL prompt. Enter your SQL query.")
        print("Enter 'exit' to exit the prompt.")

        while True:
            sql_query = input("SQL> ")
            if sql_query == "exit":
                break
            try:
                self.spark_controller.execute_sql(sql_query).show()
            except Exception as e:
                print(e)
                print("Invalid query.")

    def do_plot_scatter(self, arg):
        """Plot the frequency as scatter of all words in certain years"""
        self.spark_controller.plot_scatter()

    def do_plot_boxplot(self, arg):
        """Plot boxplot of all words in certain years"""
        scaling_factor: float = 1.0
        try:
            scaling_factor = float(input("Please enter scaling_factor (default=1.0):"))
        except:
            print("Invalid input. Set to default scaling_factor = 1.0")
        self.spark_controller.plot_box(scaling_factor)

    def do_plot_scatter_with_regression(self, arg):
        """Plot the frequency as scatter of all words in certain years and the regression line of each word"""
        self.spark_controller.plot_scatter_with_regression()

    def do_plot_kde(self, arg):
        """Plot the Kernel Density Estimation with Gauss-Kernel of a word"""
        word: str = input("Please enter word:")
        bandwidth: float = 0.25
        bins: int = 30
        try:
            bandwidth = float(input("Please enter bandwidth for Kernel Density Estimation (default=0.25):"))
        except:
            print("Invalid input. Set to default bandwidth = 0.25")
        try:
            bins = int(input("Please enter bins for Histogram (default=30):"))
        except:
            print("Invalid input. Set to default bins = 30")
        self.spark_controller.plot_kde(word, bandwidth, bins)

    def do_exit(self, arg):
        """Leave shell"""
        return True

    # overrides class method, is run before cmdloop returns but not in case the shell crashes
    def postloop(self) -> None:
        if self.spark_controller:
            self.spark_controller.close()
        print("Closed connection")
