export default function Footer({ children, pageOpts, pageProps, themeConfig, ...props } : any) {
    return (
        <footer className="flex flex-col items-center justify-center w-full h-min border-t dark:border-gb-bg2 dark:bg-gb-bg1">
            <div className="flex flex-col justify-left w-full h-full pl-[4rem] my-8 gap-4">
                <p className="text-md font-semibold text-gb-text-dark dark:text-gb-faded text-opacity-50">
                    Copyright {new Date().getFullYear()} ©{' '}
                    <a href="https://gamebeast.gg" target="_blank">
                        Gamebeast, Inc.
                    </a>
                    {" "} - All Rights Reserved.
                </p>
                <p className="text-xs text-gb-text-dark dark:text-gb-faded text-opacity-40">
                    Built with <a href="https://nextra.site" className="underline" target="_blank">Nextra</a>
                </p>
            </div>
        </footer>
    )
}