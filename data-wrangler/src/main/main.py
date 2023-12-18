from application.app import Application
from pipeline.impl.GpiPipeline import GpiPipeline


def main():
    Application(GpiPipeline).run()


if __name__ == '__main__':
    main()
